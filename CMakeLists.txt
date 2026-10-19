cmake_minimum_required(VERSION 3.20)
project(vgi-rpc-c VERSION 0.1.0 LANGUAGES C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
option(VGI_RPC_BUILD_TESTS "Build C tests and interoperability tools" ON)
option(VGI_RPC_BUILD_EXAMPLES "Build calculator client and server examples" ON)
option(VGI_RPC_SANITIZE "Enable address and undefined-behavior sanitizers" OFF)
if(VGI_RPC_SANITIZE AND NOT MSVC)
    add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address,undefined)
endif()
set(VGI_NANOARROW_SOURCE_DIR "" CACHE PATH "Use a local nanoarrow checkout")
set(VGI_NANOARROW_REVISION "4d6dbbb190d9e3fa2718086f8610b375877ed506" CACHE STRING
    "Pinned rustyconover/arrow-nanoarrow revision (IPC dictionaries and message metadata)")
set(NANOARROW_IPC ON CACHE BOOL "" FORCE)
set(NANOARROW_IPC_WITH_ZSTD ON CACHE BOOL "" FORCE)
set(NANOARROW_IPC_WITH_LZ4 ON CACHE BOOL "" FORCE)
set(NANOARROW_BUILD_TESTS OFF CACHE BOOL "" FORCE)
# nanoarrow's installed config selects its aliases using BUILD_SHARED_LIBS.
# Keep those targets installed even though vgi itself uses the static libraries.
set(NANOARROW_INSTALL_SHARED ON CACHE BOOL "" FORCE)
if(VGI_NANOARROW_SOURCE_DIR)
    add_subdirectory("${VGI_NANOARROW_SOURCE_DIR}" "nanoarrow")
else()
    include(FetchContent)
    FetchContent_Declare(nanoarrow
        GIT_REPOSITORY https://github.com/rustyconover/arrow-nanoarrow.git
        GIT_TAG "${VGI_NANOARROW_REVISION}"
        PATCH_COMMAND "${CMAKE_COMMAND}"
            "-DVGI_PATCH=${CMAKE_CURRENT_SOURCE_DIR}/cmake/patches/nanoarrow-metadata-nul.patch"
            -P "${CMAKE_CURRENT_SOURCE_DIR}/cmake/patch_nanoarrow.cmake")
    FetchContent_MakeAvailable(nanoarrow)
endif()


find_package(PkgConfig REQUIRED)
find_package(Threads REQUIRED)
pkg_check_modules(JSONC REQUIRED IMPORTED_TARGET json-c)
pkg_check_modules(SODIUM REQUIRED IMPORTED_TARGET libsodium)
pkg_check_modules(LIBEVENT REQUIRED IMPORTED_TARGET libevent libevent_pthreads)
pkg_check_modules(ZSTD REQUIRED IMPORTED_TARGET libzstd)
pkg_check_modules(CURL REQUIRED IMPORTED_TARGET libcurl)
find_package(ZLIB REQUIRED)
add_library(vgi_rpc src/schema.c src/ipc.c src/stream.c src/batch.c src/rpc.c
    src/socket.c src/credentials.c src/identity.c src/http.c src/http_codec.c src/proof.c src/session.c
    src/generated/protocol.c src/external.c src/token.c src/client.c src/client_io.c src/client_http.c src/shm.c src/pool.c src/telemetry.c)
add_library(vgi_rpc::vgi_rpc ALIAS vgi_rpc)
target_include_directories(vgi_rpc PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)
target_include_directories(vgi_rpc PRIVATE src)
target_link_libraries(vgi_rpc PUBLIC nanoarrow_ipc_static)
target_link_libraries(vgi_rpc PRIVATE PkgConfig::JSONC PkgConfig::SODIUM
    PkgConfig::LIBEVENT PkgConfig::ZSTD PkgConfig::CURL ZLIB::ZLIB Threads::Threads)
if(UNIX)
    target_link_libraries(vgi_rpc PRIVATE m)
endif()
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_link_libraries(vgi_rpc PRIVATE rt)
endif()
set_target_properties(vgi_rpc PROPERTIES WINDOWS_EXPORT_ALL_SYMBOLS ON)
if(MSVC)
    target_compile_options(vgi_rpc PRIVATE /W4)
else()
    target_compile_options(vgi_rpc PRIVATE -Wall -Wextra -Wpedantic)
endif()
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
install(TARGETS vgi_rpc EXPORT vgi_rpc-targets
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}")
install(DIRECTORY include/vgi_rpc DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
install(EXPORT vgi_rpc-targets NAMESPACE vgi_rpc:: DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/vgi_rpc")
configure_package_config_file(cmake/vgi_rpc-config.cmake.in
    "${CMAKE_CURRENT_BINARY_DIR}/vgi_rpc-config.cmake"
    INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/vgi_rpc")
write_basic_package_version_file("${CMAKE_CURRENT_BINARY_DIR}/vgi_rpc-config-version.cmake"
    VERSION "${PROJECT_VERSION}" COMPATIBILITY SameMinorVersion)
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/vgi_rpc-config.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/vgi_rpc-config-version.cmake"
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/vgi_rpc")
if(VGI_RPC_BUILD_TESTS)
    enable_testing()
    add_subdirectory(conformance)
    add_executable(vgi-rpc-security-test tests/security_test.c)
    target_include_directories(vgi-rpc-security-test PRIVATE src)
    target_link_libraries(vgi-rpc-security-test PRIVATE vgi_rpc::vgi_rpc Threads::Threads)
    add_test(NAME security COMMAND vgi-rpc-security-test)
    add_executable(vgi-rpc-client-integration tests/client_integration.c)
    target_link_libraries(vgi-rpc-client-integration PRIVATE vgi_rpc::vgi_rpc)
    add_executable(vgi-rpc-features-test tests/features_test.c conformance/protocol.c)
    target_include_directories(vgi-rpc-features-test PRIVATE src conformance)
    target_link_libraries(vgi-rpc-features-test PRIVATE vgi_rpc::vgi_rpc Threads::Threads)
    add_test(NAME runtime_features COMMAND vgi-rpc-features-test $<TARGET_FILE:conformance_worker>)
endif()

if(VGI_RPC_BUILD_EXAMPLES)
    add_executable(vgi-rpc-calculator examples/calculator.c)
    target_link_libraries(vgi-rpc-calculator PRIVATE vgi_rpc::vgi_rpc)
    add_executable(vgi-rpc-example-client examples/client.c)
    target_link_libraries(vgi-rpc-example-client PRIVATE vgi_rpc::vgi_rpc)
    if(VGI_RPC_BUILD_TESTS)
        add_test(NAME calculator COMMAND vgi-rpc-example-client $<TARGET_FILE:vgi-rpc-calculator>)
    endif()
endif()
