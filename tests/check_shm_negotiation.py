#!/usr/bin/env python3
"""Prove C clients negotiate before SHM and safely fall back with older peers."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
from pyarrow import ipc


def peer(mode: str) -> None:
    for expected in (b"__transport_options__", b"echo_string"):
        with ipc.open_stream(sys.stdin.buffer) as reader:
            batch, metadata = reader.read_next_batch_with_custom_metadata()
            assert metadata is not None and metadata[b"vgi_rpc.method"] == expected
            assert metadata[b"vgi_rpc.request_version"] == b"1"
            assert b"vgi_rpc.shm_segment_name" not in metadata
            assert b"vgi_rpc.shm_offset" not in metadata
            assert list(reader) == []
        response_metadata = {}
        if expected == b"__transport_options__":
            assert b"vgi_rpc.protocol" not in metadata
            assert metadata[b"vgi_rpc.transport.shm"] == b"true"
            assert batch.num_columns == 0 and batch.num_rows == 1
            response = pa.record_batch([], schema=pa.schema([]))
            if mode == "unsupported":
                response_metadata[b"vgi_rpc.transport.shm"] = b"false"
            elif mode == "legacy":
                response_metadata = {
                    b"vgi_rpc.log_level": b"EXCEPTION",
                    b"vgi_rpc.log_message": b"Unknown method",
                    b"vgi_rpc.log_extra": json.dumps({"exception_type": "MethodNotImplementedError"}).encode(),
                    b"vgi_rpc.error_code": b"UNIMPLEMENTED",
                    b"vgi_rpc.error_kind": b"method_not_implemented",
                }
        else:
            response = pa.record_batch([batch.column(0)], names=["result"])
        with ipc.new_stream(sys.stdout.buffer, response.schema) as writer:
            writer.write_batch(response, custom_metadata=response_metadata)
        sys.stdout.buffer.flush()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--peer", choices=("unsupported", "legacy", "missing"))
    parser.add_argument("--build-dir", type=Path, default=Path("build"))
    args = parser.parse_args()
    if args.peer:
        peer(args.peer)
        return
    test = str((args.build_dir / "vgi-rpc-features-test").resolve())
    for mode in ("unsupported", "legacy", "missing"):
        subprocess.run(
            [test, "--shm-fallback", sys.executable, str(Path(__file__).resolve()), mode],
            check=True, timeout=15,
        )
        print(f"SHM negotiation: {mode} peer stays inline without advertising a segment")


if __name__ == "__main__":
    main()
