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

# Audited optional candidate overlay. Reversal is required, not advisory.
# If the research patch or any permitted source bytes drift, this fails closed.
RESEARCH_PATCH = 'tools/research-b10-s1-d2.patch'
RESEARCH_CHANGED = {
    'protocol/multipath/core.go': '6c01d7804020ef099317c88bdeece572c23cf74712f9d9aac8c313effdd6884d',
    'protocol/multipath/inbound.go': 'e9cb8056dce6b6a48718c5862e8f4d2a52b76200ecdf7aa5d39be78eba3b01a9',
    'protocol/multipath/scheduler.go': 'f90e1fee0196dee2d20cb829832f2739846a6292394252008fe64a7cdb31ac16',
    'protocol/multipath/stream/path.go': '940da47ac9427c4d7f9d9e45f1633d1126320dbe6ae31f53b10bc36ccda6c00f',
    'protocol/multipath/stream/send.go': '9788e65272784f6bda9364c68043e12c81636a2a5d452fae6d76814dbbd9c489',
    'protocol/multipath/types.go': '61c2ec25359a21f269f3e0fcfb14f247d1d66dc92fab5bd397ae488e61408255',
    'protocol/multipath/wire.go': '369bc1d5d02530185763e987d3d52cc65976e41f888bbc5c3288058389a8613c',
}
RESEARCH_ADDED = {
    'protocol/multipath/d2_ecf.go': '795377d61b163b9cba922d044229fc8456f987d06f4afb873b20d72851dcf810',
    'protocol/multipath/d2_ecf_test.go': '5a8f2472c9f21f13a9a2807dc694de1a11242d2a41c052916094901e2cf00839',
    'protocol/multipath/stream/headroom_s1_test.go': 'd6b410ecc0672b056a8a4350fdb95006a58e2ff180644aaa0249f2e6e478a8de',
}

def normalized_sources(root):
    root = Path(root).resolve()
    with tempfile.TemporaryDirectory(prefix='download-boundary-') as temp:
        temp = Path(temp)
        for rel in set(BASELINE) | set(RESEARCH_CHANGED) | set(RESEARCH_ADDED):
            target = temp / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(root / rel, target)
        # The exact archived candidate must reverse to research-beta byte identity.
        for rel, want in RESEARCH_ADDED.items():
            if hashlib.sha256((temp / rel).read_bytes()).hexdigest() != want:
                raise RuntimeError('unexpected candidate added-file drift: ' + rel)
        subprocess.run(['git', 'apply', '--reverse', str(root / RESEARCH_PATCH)], cwd=temp, check=True)
        for rel, want in RESEARCH_CHANGED.items():
            if hashlib.sha256((temp / rel).read_bytes()).hexdigest() != want:
                raise RuntimeError('research candidate did not reverse to frozen research-beta: ' + rel)
        for rel in RESEARCH_ADDED:
            if (temp / rel).exists():
                raise RuntimeError('research candidate new file was not reversed: ' + rel)
        # Keep the original six-file downstream audit and its exact SHA list.
        subprocess.run(['git', 'apply', '--reverse', str(root / 'tools/download-observation.patch')], cwd=temp, check=True)
        result = {rel: (temp / rel).read_bytes() for rel in set(BASELINE) | set(RESEARCH_CHANGED)}
        for rel, want in BASELINE.items():
            if hashlib.sha256(result[rel]).hexdigest() != want:
                raise RuntimeError('unapproved baseline drift: ' + rel)
        return result

if __name__ == '__main__':
    normalized_sources(Path.cwd())
    print('PASS observation patch reverses to exact deployed baseline (6 files)')
