cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME airport)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

find_package(Arrow REQUIRED)
find_package(ArrowFlight REQUIRED)
find_package(msgpack-cxx CONFIG REQUIRED)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
  src/airport_flight_exception.cpp
  src/airport_extension.cpp
  src/airport_flight_stream.cpp
  src/airport_json_common.cpp
  src/airport_json_serializer.cpp
  src/airport_list_flights.cpp
  src/airport_secrets.cpp
  src/airport_take_flight.cpp
  src/storage/airport_catalog_api.cpp
  src/storage/airport_catalog_set.cpp
  src/storage/airport_catalog.cpp
  src/storage/airport_clear_cache.cpp
  src/storage/airport_schema_entry.cpp
  src/storage/airport_schema_set.cpp
  src/storage/airport_table_entry.cpp
  src/storage/airport_table_set.cpp
  src/storage/airport_transaction_manager.cpp
  src/storage/airport_transaction.cpp
  src/storage/airport_exchange.cpp
  src/storage/airport_delete.cpp
  src/storage/airport_insert.cpp
  src/storage/airport_update.cpp
  src/storage/airport_delete_parameterized.cpp
  src/storage/airport_update_parameterized.cpp
  src/airport_request_headers.cpp
  src/airport_optimizer.cpp
  src/airport_constraints.cpp
  src/airport_scalar_function.cpp
  src/airport_flight_statistics.cpp
  src/airport_schema_utils.cpp
  src/airport_action.cpp
  src/storage/airport_scalar_function_set.cpp
  src/storage/airport_table_function_set.cpp
  src/airport_telemetry.cpp
  src/airport_rpc.cpp
  src/airport_logging.cpp
)


build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})

#target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
#target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)

target_link_libraries(${EXTENSION_NAME}
  Arrow::arrow_static
  ArrowFlight::arrow_flight_static
  gRPC::grpc++
  msgpack-cxx
)

build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${LOADABLE_EXTENSION_NAME}
  Arrow::arrow_static
  ArrowFlight::arrow_flight_static
  gRPC::grpc++
  msgpack-cxx
  )


install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
