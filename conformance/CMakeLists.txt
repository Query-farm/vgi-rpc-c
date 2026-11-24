add_executable(conformance_worker worker.c nested.c identity.c secondary.c protocol.c storage.c
    state_ref.c)
target_include_directories(conformance_worker PRIVATE ../src)
target_link_libraries(conformance_worker PRIVATE vgi_rpc::vgi_rpc PkgConfig::JSONC PkgConfig::CURL PkgConfig::SODIUM)

add_executable(conformance_client_driver client_driver.c)
target_link_libraries(conformance_client_driver PRIVATE vgi_rpc::vgi_rpc PkgConfig::JSONC PkgConfig::SODIUM)

# The conformance_negotiating_client hook (PROTOCOL_VERSIONS_V1 §9).
add_executable(conformance_negotiating_client negotiating_client.c)
target_include_directories(conformance_negotiating_client PRIVATE ../src)
target_link_libraries(conformance_negotiating_client PRIVATE vgi_rpc::vgi_rpc PkgConfig::JSONC)
