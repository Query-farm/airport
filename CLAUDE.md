# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the **Airport** extension for DuckDB - a C++ extension that adds [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) protocol support to DuckDB. It enables DuckDB to query, modify, and store data via Arrow Flight servers.

## Build Commands

### Building
```sh
# Build release version
VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake GEN=ninja make release

# Build debug version
VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake GEN=ninja make debug

# Run all tests (requires building first)
make test              # runs against release build
make test_debug        # runs against debug build
```

All extension functions should be documented inside of DuckDB with CreateScalarFunctionInfo or CreateAggregateFunctionInfo or the appropriate type for the function.  This documentation of the function should include examples, parameter types and parameter names.  The function should be categorized.

When making changes the version should always be updated to the current date plus an ordinal counter in the form of YYYYMMDDCC.

It is best to test changed with a debug build as it offers more assertions and safety checks.

### Key Build Outputs
- `./build/release/duckdb` - DuckDB shell with extension pre-loaded
- `./build/release/test/unittest` - Test runner
- `./build/release/extension/airport/airport.duckdb_extension` - Loadable extension binary

## Architecture

### Extension Entry Point
- `src/airport_extension.cpp` - Main extension loading, registers all functions and the storage extension
- Extension name: "airport"

### Catalog System (DuckDB Storage Extension)
The extension implements DuckDB's catalog interface to expose remote Arrow Flight data as native database objects:

- `src/include/storage/airport_catalog.hpp` / `airport_catalog.cpp` - Main catalog implementation (`AirportCatalog`)
- `src/include/storage/airport_schema_entry.hpp` - Schema entries containing tables and functions
- `src/include/storage/airport_table_entry.hpp` - Table entries that map to Flight endpoints
- `src/include/storage/airport_transaction*.hpp` - Transaction management

### Data Operations
- `src/storage/airport_insert.cpp` - INSERT handling via DoPut
- `src/storage/airport_delete.cpp` - DELETE operations
- `src/storage/airport_update.cpp` - UPDATE operations
- `src/storage/airport_exchange.cpp` - DoExchange for bidirectional streaming

### Table Functions
- `src/airport_take_flight.cpp` - `airport_take_flight()` function to read Flight streams
- `src/airport_list_flights.cpp` - `airport_list_flights()` function to list available flights
- `src/airport_action.cpp` - Execute Flight actions

### Arrow/Flight Integration
- `src/airport_flight_stream.cpp` - Arrow Flight stream handling
- `src/airport_schema_utils.cpp` - Arrow schema <-> DuckDB type conversion
- `src/include/airport_flight_exception.hpp` - Flight error handling

### Catalog API
- `src/include/storage/airport_catalog_api.hpp` - API structures for catalog metadata
- `src/storage/airport_catalog_api.cpp` - Fetching catalog data from Flight servers

## Key Dependencies

Configured via `CMakeLists.txt`:
- Apache Arrow (static)
- Arrow Flight (static)
- gRPC
- msgpack-cxx

## Attach Syntax

```sql
ATTACH 'grpc+tls://server.example.com/database_name' AS db (TYPE airport);
ATTACH 'grpc://localhost:8815/mydb' AS local (TYPE airport, auth_token='xxx');
```

## Test Files

Tests are in `test/sql/*.test` using DuckDB's sqllogictest format. Many type-specific tests exist (`airport-test-types-*.test`).
