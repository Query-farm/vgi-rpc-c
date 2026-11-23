"""Run the blob-vendor client tests against vgi-rpc-python's fake vendor.

Starts `python -m vgi_rpc.conformance.fake_vendor --test-routes` (vgi-rpc with
the fake vendor installed in this interpreter, e.g. the reference checkout's
venv), then runs build/vgi-rpc-vendor-test with VGI_RPC_FAKE_VENDOR_URL.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, default=ROOT / "build")
    args = parser.parse_args()
    vendor = subprocess.Popen(
        [sys.executable, "-m", "vgi_rpc.conformance.fake_vendor", "--test-routes", "--token", "test-token=tenant-a"],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        url = None
        deadline = time.monotonic() + 60
        assert vendor.stdout is not None
        while time.monotonic() < deadline:
            line = vendor.stdout.readline()
            if not line:
                break
            if line.startswith("URL:"):
                url = line[4:].strip()
                break
        if url is None:
            print("fake vendor did not start", file=sys.stderr)
            return 1
        env = dict(os.environ, VGI_RPC_FAKE_VENDOR_URL=url)
        result = subprocess.run([str(args.build_dir / "vgi-rpc-vendor-test")], env=env, check=False)
        return result.returncode
    finally:
        vendor.terminate()
        vendor.wait(timeout=10)


if __name__ == "__main__":
    sys.exit(main())
