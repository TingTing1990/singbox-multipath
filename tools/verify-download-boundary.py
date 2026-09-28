#!/usr/bin/env python3
"""Verify observation patch reversibility against the supplied deployed source.
This proves baseline identity after removing approved instrumentation, NOT zero
runtime overhead or full behavioral equivalence. Go regression/parity gates are
separate and remain required.
"""
from pathlib import Path
import hashlib
import shutil
import subprocess
import tempfile

BASELINE = {'option/multipath.go': '03c7ef30cd5d4c7f2ecce0fe71a3bb088305a9deae162a481fd46ea1559ab702', 'protocol/multipath/core.go': '50577e0b8a883aeaf9321798fe0971ec19609a19e0f38ba9c5e8a253839f8e18', 'protocol/multipath/inbound.go': '1173018cd6735f7e0b6e728450108a0c8f1139e276da7404d3c9a53bf08bb5a1', 'protocol/multipath/scheduler.go': '0915d96d0a8a79f5815581513329e8c866bb9344ec1d25f570d12365518f30ca', 'protocol/multipath/transport.go': '141a760282dca81dbe516a2d8054ca0ec0bb05a00d6a6d4bed774336b2c10b57', 'protocol/multipath/types.go': '36816988d72ac672e438e41899becfd9abeb2457326de1a0de6b7ef55172349c'}

def normalized_sources(root):
    root = Path(root).resolve()
    with tempfile.TemporaryDirectory(prefix='download-boundary-') as temp:
        temp = Path(temp)
        for rel in BASELINE:
            target = temp / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(root / rel, target)
        subprocess.run(['git', 'apply', '--reverse', str(root / 'tools/download-observation.patch')], cwd=temp, check=True)
        result = {rel: (temp / rel).read_bytes() for rel in BASELINE}
        for rel, want in BASELINE.items():
            if hashlib.sha256(result[rel]).hexdigest() != want:
                raise RuntimeError('unapproved baseline drift: ' + rel)
        return result

if __name__ == '__main__':
    normalized_sources(Path.cwd())
    print('PASS observation patch reverses to exact deployed baseline (6 files)')
