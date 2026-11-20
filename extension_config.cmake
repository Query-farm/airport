# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(airport
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# httpfs pinned to the commit used by DuckDB v2.0-cyanoptera (matches the duckdb submodule)
duckdb_extension_load(httpfs
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG f71cfdb4bff85b9e15b1f3b842d9aa395aff0167
    INCLUDE_DIR extension/httpfs/include
)


# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)