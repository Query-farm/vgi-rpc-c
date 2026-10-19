# Your first C service

Build the library and call its calculator service from a native C client.
The examples use the same Arrow schemas and dispatch code over subprocess and HTTP
connections. Python is not needed for this build.

## Install dependencies

You need a C11 compiler, CMake 3.20+, and the native libraries below.

=== "macOS"

    ```sh
    brew install cmake pkg-config json-c libsodium libevent curl zstd lz4
    ```

=== "Debian / Ubuntu"

    ```sh
    sudo apt-get install build-essential cmake pkg-config libjson-c-dev \
      libsodium-dev libevent-dev libcurl4-openssl-dev libzstd-dev liblz4-dev zlib1g-dev
    ```

## Build

```sh
git clone https://github.com/Query-farm/vgi-rpc-c.git
cd vgi-rpc-c
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

CMake fetches the pinned nanoarrow fork and applies the metadata patch. See
[building](building.md) for dependency revisions, shared libraries, and local
nanoarrow checkouts.

## Call a subprocess

```sh
./build/vgi-rpc-example-client ./build/vgi-rpc-calculator
```

The client starts the worker, calls `Calculator.add` with two float64 arguments,
and prints:

```text
2 + 3 = 5
```

The client owns the subprocess and releases it when the call finishes.

## Call over HTTP

Start the same server in one terminal:

```sh
./build/vgi-rpc-calculator --http 8000
```

Then call it from another terminal:

```sh
./build/vgi-rpc-example-client --http http://127.0.0.1:8000/vgi
```

The result is again `2 + 3 = 5`. Stop the server with Ctrl-C when finished.
The [calculator walkthrough](examples.md) contains both complete C programs,
included directly from the source files that CMake compiles.

## Link your application

Install into a prefix your application can find:

```sh
cmake --install build --prefix "$HOME/.local"
```

Add the exported target to your application's `CMakeLists.txt`:

```cmake
find_package(vgi_rpc 0.1 CONFIG REQUIRED)
add_executable(my_client main.c)
target_link_libraries(my_client PRIVATE vgi_rpc::vgi_rpc)
```

Configure your application with `-DCMAKE_PREFIX_PATH="$HOME/.local"` if that
prefix is not already on CMake's search path. The target supplies nanoarrow's
headers and libraries.

Continue with [server handlers and ownership](server.md), the
[native client guide](client.md), or the [public API reference](api/index.md).
