#!/usr/bin/env python3
"""Run the canonical suite; skips outside EXPECTED_SKIP_REASONS fail the gate."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

#: The single-replica skips of vgi-rpc STATE_REF_V1.md §5.4: this port keeps HTTP
#: stream state in process, so its StateRefs are in-memory and inline only.
SINGLE_REPLICA = (
    "single-replica SDK: HTTP stream state is never serialized, so StateRefs are in-memory and inline only "
    "(STATE_REF_V1.md §5.4)"
)
#: Server-role skips the gate accepts, by ``Class::test``.  Every entry must skip
#: (a stale entry would hide coverage) and nothing else may.
EXPECTED_SKIP_REASONS = {
    "TestStateRef::test_stored_round_trip": SINGLE_REPLICA,
    "TestStateRef::test_cross_replica_resolve": SINGLE_REPLICA,
    "TestStateRef::test_foreign_and_tampered_tags_are_refused": SINGLE_REPLICA,
    "TestStateRef::test_codec": SINGLE_REPLICA,
    "TestStateRef::test_expired_tag_is_refused_before_any_get": SINGLE_REPLICA,
    "TestCursorBudget::test_warn_rule_warns_and_continues": SINGLE_REPLICA,
    "TestCursorBudget::test_error_rule_refuses": SINGLE_REPLICA,
    "TestCursorBudget::test_no_error_without_a_request_cap": SINGLE_REPLICA,
    "TestCursorBudget::test_below_the_floor_the_rule_is_off": SINGLE_REPLICA,
}


def check_skips(role: str, report: Path) -> None:
    """Fail on any skip not in ``EXPECTED_SKIP_REASONS`` and on any stale entry."""
    skipped: dict[str, str] = {}
    for case in ET.parse(report).iter("testcase"):
        marker = case.find("skipped")
        if marker is None:
            continue
        key = f"{case.get('classname', '').rsplit('.', 1)[-1]}::{case.get('name', '')}"
        skipped[key] = marker.get("message", "") + (marker.text or "")
    expected = EXPECTED_SKIP_REASONS if role == "server" else {}
    unexpected = sorted(k for k, why in skipped.items() if k not in expected or expected[k] not in why)
    stale = sorted(k for k in expected if k not in skipped)
    if unexpected or stale:
        raise SystemExit(
            f"{role} conformance is not a complete gate: unexpected skips {unexpected}, "
            f"expected skips that ran {stale}"
        )


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
            check_skips(role, report)


if __name__ == "__main__":
    main()
