#!/usr/bin/env python3
"""Linux/systemd download audit capture. Does not edit config or restart service."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import time


def sha(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def identity(service, config):
    raw = subprocess.check_output(['systemctl', 'show', service, '--property=MainPID,InvocationID,ExecMainStartTimestampMonotonic'], text=True)
    props = dict(line.split('=', 1) for line in raw.splitlines() if '=' in line)
    pid = int(props.get('MainPID', '0'))
    if pid <= 0 or not props.get('InvocationID'):
        raise RuntimeError('service is not running with an invocation identity')
    executable = '/proc/%d/exe' % pid
    return dict(pid=pid, invocation=props['InvocationID'], process_start=props.get('ExecMainStartTimestampMonotonic'),
                binary_path=os.readlink(executable), binary_sha256=sha(executable),
                version=subprocess.check_output([executable, 'version'], text=True),
                config_sha256=sha(config), boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip())


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--service', default='singbox-multipath.service')
    p.add_argument('--instance', required=True)
    p.add_argument('--config', default='/opt/singbox-multipath/config/config.json')
    p.add_argument('--output', required=True, help='new directory; existing paths are refused')
    a = p.parse_args()
    root = Path(a.output)
    root.mkdir(mode=0o700, parents=False, exist_ok=False)
    manifest = dict(schema=1, kind='systemd-server-download', instance=a.instance,
                    service=a.service, complete=False, capture_start=time.time(), config_path=a.config)
    proc = None
    journal_errors = None
    selector = None
    stop = [False]
    signal.signal(signal.SIGINT, lambda *_: stop.__setitem__(0, True))
    signal.signal(signal.SIGTERM, lambda *_: stop.__setitem__(0, True))
    try:
        manifest['start_identity'] = identity(a.service, a.config)
        entries = subprocess.check_output(['journalctl', '-u', a.service, '-n', '1', '-o', 'json', '--no-pager'], text=True)
        cursor = json.loads(entries.splitlines()[-1])['__CURSOR']
        manifest['start_cursor'] = cursor
        # A pipe reader based on raw bytes avoids text-buffer/select interactions.
        journal_errors = open(root / 'journal-stderr.txt', 'wb')
        proc = subprocess.Popen(['journalctl', '-u', a.service, '--after-cursor', cursor, '-f', '-o', 'json', '--no-pager'],
                                # Keep the journal reader alive when the terminal sends Ctrl+C to
                                # the collector group; the parent owns orderly termination.
                                stdout=subprocess.PIPE, stderr=journal_errors, start_new_session=True)
        selector = selectors.DefaultSelector()
        selector.register(proc.stdout, selectors.EVENT_READ)
        pending = b''
        ready = False
        deadline = None
        ready_deadline = time.monotonic() + 15
        with open(root / 'server-runtime.jsonl', 'wb') as output:
            while True:
                if not ready and time.monotonic() >= ready_deadline:
                    raise RuntimeError('no download snapshot within 15s; check instance, download_audit=true and info logging')
                if stop[0] and deadline is None:
                    deadline = time.monotonic() + 5
                    print('Stopping at the next aggregate snapshot...', flush=True)
                if deadline is not None and time.monotonic() >= deadline:
                    raise RuntimeError('no closing aggregate snapshot; capture incomplete')
                if proc.poll() is not None:
                    raise RuntimeError('journalctl exited before capture boundary')
                for key, _ in selector.select(.2):
                    data = os.read(key.fd, 65536)
                    if not data:
                        raise RuntimeError('journal stream ended')
                    pending += data
                    if len(pending) > 4 * 1024 * 1024:
                        raise RuntimeError('journal line buffer exceeds 4 MiB')
                    while b'\n' in pending:
                        line, pending = pending.split(b'\n', 1)
                        entry = json.loads(line)
                        message = entry.get('MESSAGE', '')
                        if isinstance(message, list):
                            try:
                                message = bytes(message).decode('utf-8')
                            except (TypeError, ValueError, UnicodeDecodeError) as exc:
                                raise RuntimeError('journal MESSAGE byte array is not valid UTF-8') from exc
                            entry['MESSAGE'] = message
                            line = json.dumps(entry, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
                        output.write(line + b'\n')
                        manifest['end_cursor'] = entry['__CURSOR']
                        if not isinstance(message, str) or 'MP_DOWNLOAD_AUDIT ' not in message:
                            continue
                        event = json.loads(message.split('MP_DOWNLOAD_AUDIT ', 1)[1])
                        if event.get('instance') != a.instance:
                            continue
                        if entry.get('_SYSTEMD_INVOCATION_ID') != manifest['start_identity']['invocation']:
                            raise RuntimeError('service invocation changed')
                        if event.get('kind') not in ('START', 'WINDOW', 'STOP'):
                            continue
                        if not ready:
                            ready = True
                            manifest['epoch'] = event['epoch']
                            manifest['first_snapshot_seq'] = event['seq']
                            print('READY: start the download test; Ctrl+C finishes capture.', flush=True)
                        elif event['epoch'] != manifest['epoch']:
                            raise RuntimeError('runtime epoch changed')
                        elif stop[0]:
                            manifest['last_snapshot_seq'] = event['seq']
                            manifest['end_identity'] = identity(a.service, a.config)
                            if manifest['end_identity'] != manifest['start_identity']:
                                raise RuntimeError('binary/config/service identity changed')
                            output.flush()
                            os.fsync(output.fileno())
                            manifest['complete'] = True
                            return 0
    except Exception as exc:
        manifest['error'] = str(exc)
        print(str(exc), file=sys.stderr)
        return 1
    finally:
        if proc is not None:
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        if selector is not None:
            selector.close()
        if proc is not None and proc.stdout is not None:
            proc.stdout.close()
        if journal_errors is not None:
            journal_errors.close()
        manifest['capture_end'] = time.time()
        journal = root / 'server-runtime.jsonl'
        if journal.exists():
            manifest['journal_sha256'] = sha(journal)
        (root / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
        files = sorted(x for x in root.iterdir() if x.is_file())
        (root / 'SHA256SUMS').write_text(''.join('%s  %s\n' % (sha(x), x.name) for x in files))
        print('Capture saved to %s; complete=%s' % (root, manifest['complete']), flush=True)


if __name__ == '__main__':
    sys.exit(main())
