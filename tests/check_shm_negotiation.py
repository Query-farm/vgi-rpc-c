#!/usr/bin/env python3
"""Prove C clients negotiate before SHM and safely fall back with older peers."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
from pyarrow import ipc
from vgi_rpc.shm import ShmSegment, resolve_shm_batch


def connection_cache(worker: str) -> None:
    schema = pa.schema([pa.field("value", pa.string(), nullable=False)])
    empty = pa.record_batch([], schema=pa.schema([]))
    segment = ShmSegment.create(1024 * 1024)
    env = {**os.environ, "VGI_RPC_SHM_MIN_BATCH_BYTES": "0"}
    server = subprocess.Popen([worker], env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    assert server.stdin is not None and server.stdout is not None

    def call(method, batch, metadata):
        metadata = {b"vgi_rpc.method": method, b"vgi_rpc.request_version": b"1", **metadata}
        with ipc.new_stream(server.stdin, batch.schema) as writer:
            writer.write_batch(batch, custom_metadata=metadata)
        server.stdin.flush()
        with ipc.open_stream(server.stdout) as reader:
            result, md = reader.read_next_batch_with_custom_metadata()
            assert md is not None and b"vgi_rpc.log_level" not in md, md
            assert list(reader) == []
        result, _, release = resolve_shm_batch(result, md, segment)
        if release:
            result = pa.RecordBatch.from_pylist(result.to_pylist(), schema=result.schema)
            release()
        return result, md

    try:
        _, md = call(b"__transport_options__", empty, {b"vgi_rpc.transport.shm": b"true"})
        assert md[b"vgi_rpc.transport.shm"] == b"true" and md[b"vgi_rpc.server_id"]
        for value in ("advertise", "cached", "replace", "cached replacement"):
            if value == "replace":
                segment.unlink()
                segment.close()
                segment = ShmSegment.create(1024 * 1024)
            batch = pa.record_batch([pa.array([value])], schema=schema)
            allocation = segment.allocate_and_write(batch)
            assert allocation is not None
            offset, length = allocation
            md = {
                b"vgi_rpc.protocol": b"ConformanceService",
                b"vgi_rpc.protocol_version": b"2.0.0",
                b"vgi_rpc.shm_offset": str(offset).encode(),
                b"vgi_rpc.shm_length": str(length).encode(),
            }
            if value in {"advertise", "replace"}:
                md[b"vgi_rpc.shm_segment_name"] = segment.name.encode()
                md[b"vgi_rpc.shm_segment_size"] = str(1024 * 1024).encode()
            result, response_md = call(b"echo_string", batch.slice(0, 0), md)
            assert result.column(0).to_pylist() == [value]
            assert b"vgi_rpc.shm_offset" in response_md
            inline_md = {k: v for k, v in md.items() if not k.startswith(b"vgi_rpc.shm_")}
            result, response_md = call(b"echo_string", batch, inline_md)
            assert result.column(0).to_pylist() == [value]
            assert b"vgi_rpc.shm_offset" not in response_md
        server.stdin.close()
        assert server.wait(timeout=5) == 0
    finally:
        if server.poll() is None:
            server.terminate()
            server.wait(timeout=5)
        server.stdout.close()
        if not server.stdin.closed:
            server.stdin.close()
        segment.unlink()
        segment.close()
    print("SHM server: handshake, cached attachment, replacement, and interleaved inline calls passed")


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
    connection_cache(str((args.build_dir / "conformance/conformance_worker").resolve()))


if __name__ == "__main__":
    main()
