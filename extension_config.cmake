# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(airport
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

duckdb_extension_load(httpfs
    LOAD_TESTS
    DONT_LINK
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG af7bcaf40c775016838fef4823666bd18b89b36b
    INCLUDE_DIR extension/httpfs/include
    )

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)