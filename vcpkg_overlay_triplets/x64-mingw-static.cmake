# Airport overlay of the vcpkg community x64-mingw-static triplet.
#
# grpc 1.76 (vcpkg 2026.06.24) emits objects with more than 2^15 sections
# (e.g. http2_client_transport.cc), which the default PE/COFF object format
# cannot hold: "Fatal error: can't write ... file too big". -mbig-obj
# switches GNU as to the big-object COFF format.
#
# Only release libraries are built, matching the *-release triplets used by
# the other CI platforms; DuckDB extensions are always built as Release.
set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)
set(VCPKG_ENV_PASSTHROUGH PATH)

set(VCPKG_CMAKE_SYSTEM_NAME MinGW)

set(VCPKG_BUILD_TYPE release)
set(VCPKG_C_FLAGS "-Wa,-mbig-obj")
set(VCPKG_CXX_FLAGS "-Wa,-mbig-obj")
