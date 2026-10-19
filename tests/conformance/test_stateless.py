"""C-specific checks that a process-local stream table cannot satisfy."""

from io import BytesIO

import httpx2
import pyarrow as pa
from conftest import spawn_http
from vgi_rpc.conformance._pytest_suite import _unary_request_body

CURSOR = b"vgi_rpc.stream_state#b64"
CALL = b"vgi_rpc.call_state#b64"
HEADERS = {
    "Content-Type": "application/vnd.apache.arrow.stream",
    "Accept-Encoding": "identity",
    "X-VGI-Accept-Encoding": "identity",
    "X-Conformance-Principal": "alice",
}


def post(port, method, body, principal="alice"):
    return httpx2.post(
        f"http://127.0.0.1:{port}/ConformanceService/{method}",
        content=body,
        headers={**HEADERS, "X-Conformance-Principal": principal},
        timeout=10,
    )


def state(response):
    assert response.status_code == 200, response.content
    with pa.ipc.open_stream(response.content) as reader:
        values = list(reader.iter_batches_with_custom_metadata())
    metadata = {k: v for value in values if value.custom_metadata for k, v in value.custom_metadata.items()}
    return values, metadata


def tick(cursor, call):
    out = BytesIO()
    with pa.ipc.new_stream(out, pa.schema([])) as writer:
        writer.write_batch(pa.record_batch([], schema=pa.schema([])), custom_metadata={CURSOR: cursor, CALL: call})
    return out.getvalue()


def test_continuation_on_another_process_and_token_binding():
    flags = ("--no-call-state-cache", "--token-key", "91" * 32, "--sticky-auth")
    with spawn_http(*flags) as first, spawn_http(*flags) as second:
        _, initial = state(post(first, "produce_n/init", _unary_request_body("produce_n", count=5)))
        _, other = state(post(first, "produce_n/init", _unary_request_body("produce_n", count=8)))
        values, resumed = state(post(second, "produce_n/exchange", tick(initial[CURSOR], initial[CALL])))
        assert values[0].batch.column("value").to_pylist() == [10]
        assert resumed[CURSOR] != initial[CURSOR]
        assert CALL not in resumed
        assert post(second, "produce_n/exchange", tick(initial[CURSOR], other[CALL])).status_code == 400
        assert post(second, "produce_n/exchange", tick(initial[CURSOR], b"")).status_code == 400
        assert post(second, "produce_n/exchange", tick(initial[CURSOR], initial[CALL]), "bob").status_code == 400
        assert post(second, "produce_with_logs/exchange", tick(initial[CURSOR], initial[CALL])).status_code == 400
        corrupt = bytearray(initial[CURSOR])
        corrupt[20] = ord("A") if corrupt[20] != ord("A") else ord("B")
        assert post(second, "produce_n/exchange", tick(bytes(corrupt), initial[CALL])).status_code == 400
        values, _ = state(post(first, "produce_n/exchange", tick(resumed[CURSOR], initial[CALL])))
        assert values[0].batch.column("value").to_pylist() == [20]
