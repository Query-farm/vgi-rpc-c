"""C-specific protocol 3.4 large-value checks the shared suite cannot see.

The shared ``_state_ref_pytest`` cases that need serialized stream state skip
for this single-replica port (STATE_REF_V1.md §5.4); these cover what the C
server still owes over the wire: decoding presented inline StateRefs and
refusing others with a 400, the upload cap equalling the fetch cap (fix 1),
the grant columns, and the 16 KiB floor warning.
"""

import subprocess

import zstandard
from conftest import _wait_for_http, spawn_http, worker_path
from vgi_rpc.conformance._state_ref_pytest import _client, _exchange, _grant, _stream


def test_presented_inline_refs_decode_and_others_are_400(conformance_http_port):
    """Inline refs (raw and zstd) resolve; stored, unknown-form and bad-codec refs are 400s."""
    with _client(conformance_http_port) as client:
        cursor, call = _stream(client, size=64, seed="c-local")
        own = _exchange(client, cursor, call)
        assert own.row["size"] == 64 and own.row["form"] == "memory" and own.row["ref"] is None
        raw = _exchange(client, own.cursor, call, override=b"\x01\x00hello")
        assert raw.row["size"] == 5
        zstd = _exchange(client, raw.cursor, call, override=b"\x01\x01" + zstandard.compress(b"z" * 4096))
        assert zstd.row["size"] == 4096
        for override in (b"\x02\x01" + b"\x00" * 64, b"\x07\x00x", b"\x01\x02x", b"\x01\x7fx"):
            cursor, call = _stream(client, size=64, seed="c-local")
            reply = _exchange(client, cursor, call, override=override)
            assert reply.error is not None and reply.error.error_kind == "invalid_state_ref"
            assert reply.status == 400


def test_upload_cap_is_the_fetch_cap(conformance_fake_storage):
    """Fix 1: VGI-Max-Upload-Bytes is max_fetch_bytes, and grants above it are refused."""
    with (
        spawn_http("--fake-storage", conformance_fake_storage, "--max-fetch-bytes", "5000") as port,
        _client(port) as client,
    ):
        assert client.get("/health").headers["VGI-Max-Upload-Bytes"] == "5000"
        reply, raw = _grant(client, b"x" * 5001, principal=None)
        assert raw.status_code == 400 and "exceeds VGI-Max-Upload-Bytes (5000)" in str(reply.error)
        reply, raw = _grant(client, b"x" * 5000, principal=None)
        row = reply.row
        assert raw.status_code == 200
        assert isinstance(row["object_id"], str) and row["object_id"]
        assert isinstance(row["ref"], bytes) and len(row["ref"]) > 41


def test_request_cap_below_the_floor_warns():
    """A cap below 16 KiB starts, with the floor warning on stderr."""
    proc = subprocess.Popen(
        [worker_path(), "--http", "--max-request-bytes", "9000"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None and proc.stderr is not None
        line = proc.stdout.readline().decode().strip()
        assert line.startswith("PORT:"), line
        _wait_for_http(int(line.split(":", 1)[1]))
    finally:
        proc.terminate()
        _, err = proc.communicate(timeout=10)
    assert b"max_request_bytes (9000) is below 16384: the cursor rule is disabled" in err, err
