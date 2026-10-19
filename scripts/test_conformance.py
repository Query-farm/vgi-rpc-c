#!/usr/bin/env python3
"""Run the canonical suite; skipped tests fail the gate."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", choices=("server", "client", "both"), default="both")
    parser.add_argument("--build-dir", type=Path, default=Path("build"))
    parser.add_argument("--reference-repo", type=Path, default=Path.home() / "Development/vgi-rpc")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    build = args.build_dir.resolve()
    env = {
        **os.environ,
        "VGI_RPC_C_WORKER": str(build / "conformance/conformance_worker"),
        "VGI_CLIENT_DRIVER": str(build / "conformance/conformance_client_driver"),
        "VGI_RPC_PYTHON_REPO": str(args.reference_repo.resolve()),
        "VGI_RPC_PYTHON": sys.executable,
    }
    for name in ("VGI_RPC_C_WORKER", "VGI_CLIENT_DRIVER"):
        if not Path(env[name]).is_file():
            raise SystemExit(f"Build the conformance tools first: {env[name]}")
    roles = ("server", "client") if args.role == "both" else (args.role,)
    with tempfile.TemporaryDirectory(prefix="vgi-c-conformance-") as temp:
        for role in roles:
            report = Path(temp) / f"{role}.xml"
            subprocess.run(
                [sys.executable, "-m", "pytest", "tests/conformance", "-q", f"--junitxml={report}"],
                cwd=root,
                env={
                    **env,
                    "VGI_CONFORMANCE_ROLE": role,
                    "VGI_CONFORMANCE_SERVER": "python" if role == "client" else "c_port",
                },
                check=True,
            )
            if ET.parse(report).findall(".//skipped"):
                raise SystemExit(f"{role} conformance skipped tests; this is not a complete gate")


if __name__ == "__main__":
    main()
