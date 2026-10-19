# Conformance

The authoritative shared suite is in the
[Python reference](https://github.com/Query-farm/vgi-rpc-python).
Validation on macOS arm64, 2026-10-19, used reference commit
`73108975795d63dd8d1da03117b286d7a4aeb6ef`.

| Gate | Result |
| --- | --- |
| Complete shared suite, C server | 1,692 passed; no failures or skips |
| Additional C cross-worker token test | 1 passed |
| Native C client against Python reference | 1,397 passed; no failures or skips; 296 server-only/local tests deselected |
| Shared suite with ASan + UBSan | 1,693 passed |
| Native C client with ASan + UBSan | 1,397 passed |
| Runtime feature tests | Shared memory, worker pooling, telemetry queues and failures passed |
| OTLP/HTTP collector | Protobuf validation, context, metrics, retries and rejections passed |
| Standalone CLI, stdio / Unix / TCP | 107 passed, 6 transport/fixture-dependent skips on each |
| Standalone CLI, HTTP | 110 passed, 3 transport/fixture-dependent skips |
| Native C batch construction | Null, empty, and populated nested/dictionary/decimal/timestamp batches passed |
| Native HTTP producer state machine | Empty, zero-row application data, emit-and-finish, buffered init passed |
| Native HTTPS | Default trust rejection, configured CA acceptance, wrong-hostname rejection passed |

The CLI's transport/fixture-dependent skips are separate from the complete portable
pytest gate. The CLI includes the large-batch cases, including raw batches over
2 GiB. The shared suite covers pipe, owned/shared subprocess, Unix, TCP, HTTP,
and HTTP with every eligible response externalized. The SHM fixture transfers
large batches through the shared-memory side channel; small/non-fitting batches
retain inline fallback.

## Reproduce the gates

Build the worker and client driver with the default CMake options. Use a Python
3.13+ environment containing the reference checkout and its `http`, `conformance`,
`mtls`, `external`, and `otel` extras, plus pytest, pytest-timeout, and opentelemetry-proto.

```sh
../vgi-rpc/.venv/bin/python scripts/test_conformance.py --reference-repo ../vgi-rpc
../vgi-rpc/.venv/bin/python tests/check_native_client.py
../vgi-rpc/.venv/bin/python tests/check_shm_negotiation.py
../vgi-rpc/.venv/bin/python tests/check_telemetry.py
../vgi-rpc/.venv/bin/python scripts/generate_protocol.py --check
ctest --test-dir build --output-on-failure
```

`test_conformance.py` runs server and native-client roles and fails if either
skips a test. Client-role deselection is deliberate: raw HTTP probes of a server
must not be counted as proof of a client's behavior. Its JSONL driver obeys the
[canonical driver contract](https://github.com/Query-farm/vgi-rpc-python/blob/main/tools/cross-port/specs/CLIENT_DRIVER_PROTOCOL.md):
transport handling, continuations, fetching, error decoding, and logs all live
in the C library.

`tests/client_integration.c` constructs its own Arrow batches. Its Python launcher
only starts independent peers. It covers raw transports, mounted HTTP, buffered
producer init, external uploads/downloads, and HTTPS. The native security test
covers proof verification/replay windows, session ownership and identity binding,
concurrent session resumes, token compression, and token purpose/route binding.
The additional HTTP test resumes on another process and rejects missing, swapped,
tampered, and differently authenticated tokens.

## Sanitizers

```sh
cmake -S . -B build-sanitize -DVGI_RPC_SANITIZE=ON -DCMAKE_BUILD_TYPE=Debug
cmake --build build-sanitize --parallel
ctest --test-dir build-sanitize --output-on-failure
ASAN_OPTIONS=quarantine_size_mb=1:thread_local_quarantine_size_kb=64 \
  ../vgi-rpc/.venv/bin/python scripts/test_conformance.py \
  --build-dir build-sanitize --reference-repo ../vgi-rpc
```

The bounded ASan quarantine keeps the resource-soak RSS tests meaningful;
otherwise the sanitizer's deliberate freed-memory quarantine appears as growth.

## Scope

Additional native tests exercise actual shared-memory transfer in both directions,
allocation bounds, retained-result ownership, exhaustion fallback, pool capacity,
concurrency, idle eviction, crash/abandoned-stream recovery, and shutdown with leases.
OTLP collector tests validate the official protobuf schemas, parent/child traces,
sampling, error status, metrics, and correlated logs over stdio and HTTP.
Exporter stalls, queue overflow, and failures leave RPC results unaffected.

The portable gate does not imply support for optional cloud deployments,
native Iroh, or OAuth/cloud credential-provider SDKs. HTTPS clients use libcurl; HTTPS hosting uses a
reverse proxy. External storage is provided through allocator callbacks.
The website capability matrix records these boundaries separately for clients
and servers.
