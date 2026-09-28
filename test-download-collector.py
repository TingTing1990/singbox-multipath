#!/usr/bin/env python3
"""LOCAL integration: real runtime emitter fixture, mocked systemd journal pipe.
This does not claim a real systemd FIELD capture on the target VPS.
"""
import importlib.util
import json
import os
from pathlib import Path
import signal
import select
import time
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('collector', Path(__file__).with_name('collect-multipath-download.py'))
collector = importlib.util.module_from_spec(spec)
spec.loader.exec_module(collector)
FIXTURE = Path(os.environ['DOWNLOAD_AUDIT_EVIDENCE_DIR']) / 'producer.log'
OFFLINE = os.environ['DOWNLOAD_AUDIT_OFFLINE_BIN']
IDENTITY = dict(pid=123, invocation='invocation', process_start='100', binary_path='/proc/123/exe', binary_sha256='1' * 64,
                config_sha256='2' * 64, boot_id='boot', version='local runtime producer fixture')


class PipeJournal:
    def __init__(self, lines, byte_array_messages=False):
        r, w = os.pipe()
        self.stdout = os.fdopen(r, 'rb', buffering=0)
        self.returncode = None
        def send():
            try:
                with os.fdopen(w, 'wb') as f:
                    for i, message in enumerate(lines):
                        journal_message = list(message.encode('utf-8')) if byte_array_messages else message
                        f.write(json.dumps(dict(MESSAGE=journal_message, __CURSOR='cursor-%d' % i,
                                                _SYSTEMD_INVOCATION_ID='invocation', _BOOT_ID='boot')).encode() + b'\n')
                    f.flush()
            except BrokenPipeError:
                pass
        self.thread = threading.Thread(target=send, daemon=True)
        self.thread.start()
    def poll(self):
        return self.returncode
    def terminate(self):
        self.returncode = 0
        self.stdout.close()
    kill = terminate
    def wait(self, timeout=None):
        self.thread.join(timeout)
        return 0


class CollectorTests(unittest.TestCase):
    def collect(self, root, identity_change=False, byte_array_messages=False):
        lines = FIXTURE.read_text().splitlines()
        handlers = {}
        process = PipeJournal(lines, byte_array_messages=byte_array_messages)
        def printed(*args, **kwargs):
            if args and str(args[0]).startswith('READY:'):
                handlers[signal.SIGINT]()
        changes = [IDENTITY, {**IDENTITY, 'invocation': 'changed'}] if identity_change else [IDENTITY, IDENTITY]
        argv = ['collector', '--instance', 'mp-in', '--output', str(root), '--config', 'unused']
        with patch.object(sys, 'argv', argv), patch.object(collector, 'identity', side_effect=changes), \
             patch.object(collector.subprocess, 'check_output', return_value='{"__CURSOR":"before"}\n'), \
             patch.object(collector.subprocess, 'Popen', return_value=process), \
             patch.object(collector.signal, 'signal', side_effect=lambda signum, fn: handlers.__setitem__(signum, fn)), \
             patch('builtins.print', side_effect=printed):
            return collector.main()

    def test_runtime_collector_offline_and_tampering(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d) / 'capture'
            self.assertEqual(self.collect(root), 0)
            command = [OFFLINE, '-journal', str(root / 'server-runtime.jsonl'), '-manifest', str(root / 'manifest.json'), '-instance', 'mp-in']
            run = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(run.returncode, 0, run.stderr)
            report = json.loads(run.stdout)
            self.assertTrue(report['Complete'])
            self.assertEqual(sum(w['Delta']['ConfirmedLogical'] for w in report['Windows']), 32768)
            manifest = json.loads((root / 'manifest.json').read_text())
            for field, value in [('end_cursor', 'absent'), ('epoch', 'other'), ('complete', False), ('journal_sha256', '0'*64), ('last_snapshot_seq', 999999)]:
                altered = {**manifest, field: value}
                (root / 'manifest.json').write_text(json.dumps(altered))
                result = subprocess.run(command, capture_output=True, text=True)
                self.assertNotEqual(result.returncode, 0, field)

            byte_root = Path(d) / 'capture-byte-message'
            self.assertEqual(self.collect(byte_root, byte_array_messages=True), 0)
            normalized = [json.loads(line) for line in (byte_root / 'server-runtime.jsonl').read_text().splitlines()]
            self.assertTrue(normalized)
            self.assertTrue(all(isinstance(entry.get('MESSAGE'), str) for entry in normalized))
            byte_command = [OFFLINE, '-journal', str(byte_root / 'server-runtime.jsonl'), '-manifest', str(byte_root / 'manifest.json'), '-instance', 'mp-in']
            byte_run = subprocess.run(byte_command, capture_output=True, text=True)
            self.assertEqual(byte_run.returncode, 0, byte_run.stderr)
            byte_report = json.loads(byte_run.stdout)
            self.assertTrue(byte_report['Complete'])
            self.assertEqual(sum(w['Delta']['ConfirmedLogical'] for w in byte_report['Windows']), 32768)

    @unittest.skipUnless(os.name == 'posix', 'terminal process groups require POSIX')
    def test_terminal_ctrl_c_preserves_closing_window(self):
        # Real processes and a process-group SIGINT, unlike the earlier direct
        # Python handler invocation. journalctl must survive the parent's Ctrl+C.
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            lines = FIXTURE.read_text().splitlines()
            journal = [json.dumps(dict(MESSAGE=line, __CURSOR='real-%d' % i,
                       _SYSTEMD_INVOCATION_ID='invocation', _BOOT_ID='boot')) for i, line in enumerate(lines)]
            writer = root / 'journal_writer.py'
            writer.write_text('import time\nlines=' + repr(journal) + '\nfor i,line in enumerate(lines):\n print(line,flush=True)\n if i==0: time.sleep(0.5)\ntime.sleep(30)\n')
            runner = root / 'runner.py'
            runner.write_text("import importlib.util,subprocess,sys\nspec=importlib.util.spec_from_file_location('collector'," + repr(str(Path(collector.__file__).resolve())) + ")\nc=importlib.util.module_from_spec(spec)\nspec.loader.exec_module(c)\nc.identity=lambda *a:" + repr(IDENTITY) + "\nc.subprocess.check_output=lambda *a,**k:'{\"__CURSOR\":\"before\"}\\n'\nreal_popen=subprocess.Popen\nc.subprocess.Popen=lambda args,**kw:real_popen([sys.executable,'-u'," + repr(str(writer)) + "],**kw)\nsys.exit(c.main())\n")
            proc = subprocess.Popen([sys.executable, '-B', '-u', str(runner), '--instance', 'mp-in', '--config', 'unused', '--output', str(root/'capture')], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, start_new_session=True)
            try:
                readable, _, _ = select.select([proc.stdout], [], [], 10)
                self.assertTrue(readable, 'collector failed to signal readiness')
                ready = proc.stdout.readline()
                self.assertTrue(ready.startswith('READY:'), ready)
                os.killpg(proc.pid, signal.SIGINT)
                stdout, stderr = proc.communicate(timeout=10)
                self.assertEqual(proc.returncode, 0, stdout + stderr)
                manifest = json.loads((root/'capture/manifest.json').read_text())
                self.assertTrue(manifest['complete'])
            finally:
                if proc.poll() is None:
                    os.killpg(proc.pid, signal.SIGTERM)
                    proc.communicate(timeout=5)

    def test_service_identity_change_fails(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d) / 'capture'
            self.assertEqual(self.collect(root, True), 1)
            self.assertFalse(json.loads((root / 'manifest.json').read_text())['complete'])


if __name__ == '__main__':
    unittest.main()
