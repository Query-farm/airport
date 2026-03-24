# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(airport
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

#duckdb_extension_load(httpfs
#    LOAD_TESTS
#    DONT_LINK
#    GIT_URL https://github.com/duckdb/duckdb-httpfs
#    GIT_TAG 354d3f436a33f80f03a74419e76eb59459e19168
#    INCLUDE_DIR extension/httpfs/include
#)

duckdb_extension_load(httpfs
    LOAD_TESTS
    DONT_LINK
    INCLUDE_DIR extension/httpfs/include
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 7e86e7a5e5a1f01f458361bebdfa9b0a9a73a619
)


# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)