add_executable(conformance_worker worker.c nested.c identity.c secondary.c protocol.c storage.c)
target_include_directories(conformance_worker PRIVATE ../src)
target_link_libraries(conformance_worker PRIVATE vgi_rpc::vgi_rpc PkgConfig::JSONC PkgConfig::CURL)

add_executable(conformance_client_driver client_driver.c)
target_link_libraries(conformance_client_driver PRIVATE vgi_rpc::vgi_rpc PkgConfig::JSONC PkgConfig::SODIUM)
