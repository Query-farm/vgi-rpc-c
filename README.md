<p align="center">
  <img src="https://vgi-rpc-python.query.farm/assets/logo-shield.png" alt="Vector Gateway Interface logo" width="280">
</p>

<h1 align="center">vgi-rpc-c</h1>

<p align="center">
  C11 clients and servers for the <a href="https://vgi-rpc.query.farm">vgi-rpc</a> Arrow IPC protocol.<br>
  Built by <a href="https://query.farm">🚜 Query.Farm</a>
</p>

<p align="center">
  <a href="https://github.com/Query-farm/vgi-rpc-c/actions/workflows/ci.yml"><img src="https://github.com/Query-farm/vgi-rpc-c/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue" alt="Apache 2.0"></a>
  <img src="https://img.shields.io/badge/C-11-blue" alt="C11">
</p>

Build services and clients around Arrow C Data schemas and arrays. The runtime
uses [nanoarrow](https://github.com/rustyconover/arrow-nanoarrow) for Arrow IPC,
including dictionaries, nested types, decimals, and per-batch metadata. The
library and its Arrow dependencies compile as C.

This is the reusable RPC layer used by [vgi-c](https://github.com/Query-farm/vgi-c),
which adds VGI functions, catalogs, filters, COPY, and DuckDB worker behavior.
Python supplies the canonical protocol definitions and interoperability tests;
it is not needed to build or run an application.

**Features**

- Unary calls, producer streams, and bidirectional exchanges, including stream headers, cancellation, in-band logs, and structured errors.
- Standard input/output, owned subprocesses, Unix sockets, raw TCP, and HTTP. HTTPS clients verify certificates and hostnames through libcurl.
- Co-hosted protocols and `vgi_rpc.Reflection.v1` service discovery.
- Stateless HTTP continuations with separate authenticated call and cursor tokens, identity binding, compression, and bounded decoding.
- HTTP bearer and callback authentication, Identity/grant operations, proxy proofs, sticky sessions, CORS, compression, response budgets, and access logs.
- External payload publication and resolution with method-specific upload/download URLs, digest verification, redirect validation, and fetch/decompression limits.

Shared-memory acceleration, native Iroh connectors, worker pools, and
OpenTelemetry exporters are not implemented. SHM-capable peers use the inline
fallback. See [capabilities and conformance](docs/conformance.md) for the tested
scope and [hosting](docs/hosting.md) for HTTP deployment options.

## Installation

Requires CMake 3.20+, a C11 compiler, pkg-config, json-c, libsodium, libevent,
libcurl, zstd, LZ4, zlib, and platform threads. POSIX platforms are supported.

```sh
# macOS
brew install cmake pkg-config json-c libsodium libevent curl zstd lz4

# Debian / Ubuntu
sudo apt-get install build-essential cmake pkg-config libjson-c-dev \
  libsodium-dev libevent-dev libcurl4-openssl-dev libzstd-dev liblz4-dev zlib1g-dev

git clone https://github.com/Query-farm/vgi-rpc-c.git
cd vgi-rpc-c
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

CMake fetches the pinned nanoarrow fork and applies the embedded-NUL metadata
patch. A local checkout can be selected with `-DVGI_NANOARROW_SOURCE_DIR=/path/to/nanoarrow`;
it must already contain those APIs and fixes. See [building](docs/building.md).

## Quick start

The [calculator server](examples/calculator.c) declares an `add(a, b)` method,
constructs its schemas using nanoarrow, and registers a C handler. The
[client](examples/client.c) builds a parameter batch and reads the returned value.
Both examples are compiled by the default build:

```sh
./build/vgi-rpc-example-client ./build/vgi-rpc-calculator
# 2 + 3 = 5

# In another terminal, expose the same service over HTTP:
./build/vgi-rpc-calculator --http 8000
./build/vgi-rpc-example-client --http http://127.0.0.1:8000/vgi
```

The same call uses the native client API on either transport:

```c
struct VgiClientOptions options;
VgiClientOptionsInit(&options);
options.transport = VGI_CLIENT_HTTP;
options.target = "http://127.0.0.1:8000/vgi";
options.protocol = "Calculator";
options.protocol_version = "1.0.0";

struct VgiClient *client = NULL;
struct ArrowError error = {{0}};
int rc = VgiClientConnect(&options, &client, &error);
struct VgiAnnotatedBatch request = {0}, response = {0};
/* Build request.batch from the declared parameter schema; see examples/client.c. */
if (!rc) rc = VgiClientUnary(client, "add", &request, &response);
VgiAnnotatedBatchReset(&response);
VgiClientFree(client);
```

## Use from CMake

```sh
cmake --install build --prefix "$HOME/.local"
```

```cmake
find_package(vgi_rpc 0.1 CONFIG REQUIRED)
add_executable(my_client main.c)
target_link_libraries(my_client PRIVATE vgi_rpc::vgi_rpc)
```

The exported target supplies nanoarrow's headers and libraries. Headers also
support C++ callers through `extern "C"`.

## Documentation

- [Overview and API map](docs/index.md)
- [Building and dependency versions](docs/building.md)
- [Server handlers, streams, and ownership](docs/server.md)
- [Native clients, errors, and sessions](docs/client.md)
- [HTTP hosting and external storage](docs/hosting.md)
- [Conformance and reproducible validation](docs/conformance.md)
- [Canonical wire protocol](https://github.com/Query-farm/vgi-rpc-python/blob/main/docs/WIRE_PROTOCOL.md)

## Protocol schemas

`scripts/generate_protocol.py` reads the Python reference's Reflection, Identity,
and conformance protocols through the Arrow C Data Interface. It preserves
field flags, nested layouts, dictionary schemas, and metadata. Generated C files
are committed, so normal builds do not run a generator.

```sh
# Use the Python environment from the canonical vgi-rpc checkout.
../vgi-rpc/.venv/bin/python scripts/generate_protocol.py --check
```

Application protocols can provide schema factories directly, as the calculator
does. VGI-specific schema generation remains in `vgi-c` and reads `vgi-python`.

## Validation

The complete shared server suite passes without skips. A separate native-client
run drives this C library against the Python reference server; the JSONL driver
only relays requests and results. C-only integration tests also construct the
nullable, dictionary, timestamp, decimal, list, and struct batches themselves.

```sh
../vgi-rpc/.venv/bin/python scripts/test_conformance.py --reference-repo ../vgi-rpc
../vgi-rpc/.venv/bin/python tests/check_native_client.py
```

See [the validation report](docs/conformance.md) for source revisions, counts,
transport coverage, and sanitizer instructions.

## License

[Apache License 2.0](LICENSE). Copyright Query Farm LLC.
