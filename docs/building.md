# Building

The runtime requires C11 and POSIX process/socket APIs. CMake 3.20 or newer
configures the build. The linked native dependencies are nanoarrow IPC, json-c,
libsodium, libevent with pthread support, libcurl, zstd, LZ4, zlib, and threads.
No Arrow C++ or DuckDB library is linked.

## nanoarrow source

The default build fetches `rustyconover/arrow-nanoarrow` at
`4d6dbbb190d9e3fa2718086f8610b375877ed506`, which supplies the dictionary IPC and
message-metadata APIs. It applies
[`nanoarrow-metadata-nul.patch`](https://github.com/Query-farm/vgi-rpc-c/blob/main/cmake/patches/nanoarrow-metadata-nul.patch),
from fork commit `d83a538b1c0442d9fe4f5ecf697ce681893bed2e`, to preserve embedded
NUL bytes in metadata. These changes currently live on separate fork branches.

`VGI_NANOARROW_SOURCE_DIR` selects a local checkout. Local sources are used as
supplied, so include both changes before building. `VGI_NANOARROW_REVISION`
overrides the fetched revision. Nanoarrow's CMake probes a C++ compiler; the
runtime and bundled Arrow source files still compile as C.

## Build options

| Option | Default | Purpose |
| --- | --- | --- |
| `BUILD_SHARED_LIBS` | OFF | Build the RPC library as a shared library |
| `VGI_RPC_BUILD_TESTS` | ON | Native tests and conformance worker/client driver |
| `VGI_RPC_BUILD_EXAMPLES` | ON | Calculator server and client |
| `VGI_RPC_SANITIZE` | OFF | AddressSanitizer and UndefinedBehaviorSanitizer |
| `VGI_NANOARROW_SOURCE_DIR` | empty | Use a local nanoarrow checkout |

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
ctest --test-dir build --output-on-failure
cmake --install build --prefix "$HOME/.local"
```

Consumers use `find_package(vgi_rpc 0.1 CONFIG REQUIRED)` and link
`vgi_rpc::vgi_rpc`. Set `CMAKE_PREFIX_PATH` when installing to a custom prefix.
The installation includes nanoarrow's package and public headers. An embedded
build can instead use `add_subdirectory(/path/to/vgi-rpc-c rpc)`.

## Schema generation

The Python reference checkout supplies the canonical Reflection, Identity, and
conformance declarations. Use its environment to run:

```sh
../vgi-rpc/.venv/bin/python scripts/generate_protocol.py
../vgi-rpc/.venv/bin/python scripts/generate_protocol.py --check
```

Application schema factories need no Python runtime. The generated source is a
set of C declarations preserving the Arrow C Data representation.
