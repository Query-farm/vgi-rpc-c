# Working on vgi-rpc-c

## Scope and layout

This repository is the reusable C11 RPC library. It owns Arrow IPC, clients,
transports, shared memory, worker pools, telemetry, reflection, identity, and
external payloads. VGI catalogs, function execution, COPY, and attach tickets
belong in the sibling `vgi-c` repository.

- `include/vgi_rpc/`: public API and generated protocol declarations.
- `src/`: runtime implementation; internal headers stay here.
- `conformance/`: native worker and client driver for the Python reference suite.
- `tests/`: native tests, interoperability launchers, and conformance fixtures.
- `examples/`: compiled calculator server/client examples.
- `docs/`: build, ownership, transport, and validation guides.

## Build and dependencies

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DBUILD_SHARED_LIBS=ON
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

Keep the runtime and Arrow integration in C. Python is used for development and
interoperability tests. The CMake build pins the nanoarrow fork and applies its
embedded-NUL metadata fix. A checkout selected with `VGI_NANOARROW_SOURCE_DIR`
is used as supplied and must already include the required APIs and fix; see
`docs/building.md`. Do not casually advance dependency or reference-test pins.

## Schemas and ownership

The canonical RPC schemas come from the `vgi-rpc-python` reference, usually
checked out at `../vgi-rpc`. Regenerate with its environment:

```sh
../vgi-rpc/.venv/bin/python scripts/generate_protocol.py
../vgi-rpc/.venv/bin/python scripts/generate_protocol.py --check
```

Edit the generator or canonical model rather than generated C declarations.
Preserve schema flags, dictionaries, field metadata, and embedded NUL bytes.
Follow the public headers' ownership rules: borrowed inputs, owned outputs, and
release/reset on every error path. Keep protocol stdout free of diagnostic text.
Format changed C files using `.clang-format`; avoid unrelated formatting churn.

## Validation

Choose checks appropriate to the change. For transport, framing, client, or
server behavior, run native tests and both conformance roles:

```sh
../vgi-rpc/.venv/bin/python scripts/test_conformance.py --reference-repo ../vgi-rpc
../vgi-rpc/.venv/bin/python tests/check_native_client.py
../vgi-rpc/.venv/bin/python tests/check_shm_negotiation.py
../vgi-rpc/.venv/bin/python tests/check_telemetry.py
../vgi-rpc/.venv/bin/ruff check scripts tests
```

The reference environment needs the extras listed in `docs/conformance.md`.
Conformance skips fail the gate; client-role deselection of server-only tests is
intentional. Do not replace native client behavior with Python forwarding to make
tests pass. Check shared-memory negotiation, cached attachments, interleaved
inline calls, and retained results when changing that transport.

Use `-DVGI_RPC_SANITIZE=ON` for memory/ownership changes. The full sanitizer
conformance run uses `ASAN_OPTIONS=quarantine_size_mb=1:thread_local_quarantine_size_kb=64`
as documented. Capture long test output and inspect that log before rerunning;
repeat after relevant changes or failures, not merely to obtain another summary.

For documentation changes, check links and `git diff --check`. For installation
changes, stage `cmake --install` under a temporary prefix and inspect the files.
Update public documentation and capability claims when behavior changes.

The documentation website is built from this repository with MkDocs Material.
Install `requirements-docs.txt` into `.venv-docs`, then run
`.venv-docs/bin/mkdocs build --strict` (or `serve` for a preview). Keep all pages
in `mkdocs.yml` navigation. Examples and API declarations use source includes;
edit their C files rather than duplicating them in Markdown. The `Docs` workflow
validates pull requests and publishes `main` to `https://vgi-rpc-c.pages.dev/`.

## License

The repository uses the Query Farm Source-Available License, Version 1.0, matching
`../vgi-python/LICENSE` exactly. Keep the README and first-party notices consistent
with it. Dependencies, including nanoarrow and flatcc, retain their own licenses.
