<p align="center">
  <a href="https://query.farm">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://query.farm/media-kit/logo/wordmark-dark.svg">
      <img alt="Query.Farm" src="https://query.farm/media-kit/logo/wordmark-light.svg" height="64">
    </picture>
  </a>
  &nbsp;&nbsp;
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="logos/duckdb-inline-dark.svg">
    <img alt="DuckDB" src="logos/duckdb-inline-light.svg" height="48">
  </picture>
</p>

# Airport Extension for DuckDB

[![DuckDB Extension](https://query.farm/media-kit/shields/duckdb-extension.svg)](https://query.farm/products/extensions/airport)
[![DuckDB](https://img.shields.io/badge/DuckDB-community_extension-fdf1e0?logo=duckdb&logoColor=fff000)](https://duckdb.org/community_extensions/extensions/airport.html)
[![v1.5 build](https://github.com/Query-farm/airport/actions/workflows/MainDistributionPipeline.yml/badge.svg?branch=v1.5)](https://github.com/Query-farm/airport/actions/workflows/MainDistributionPipeline.yml?query=branch%3Av1.5)

The **Airport** extension brings [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) support to [DuckDB](https://duckdb.org), enabling DuckDB to query, modify, and store data via Arrow Flight servers.

## Documentation

Full documentation, including installation, usage, the function reference, and cookbook examples, is available at:

**[https://query.farm/products/extensions/airport](https://query.farm/products/extensions/airport)**

## Installation

```sql
INSTALL airport FROM community;
LOAD airport;
```

# Building the extension

```sh
# Clone this repo with submodules.
# duckdb and extension-ci-tools are submodules.
git clone --recursive git@github.com:Query-farm/airport

# Clone the vcpkg repo
git clone https://github.com/Microsoft/vcpkg.git

# Bootstrap vcpkg
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake

# Build the extension
make

# If you have ninja installed, you can use it to speed up the build
# GEN=ninja make
```

The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/airport/airport.duckdb_extension
```

- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `airport.duckdb_extension` is the loadable binary as it would be distributed.

## Building on MacOS
If you have difficulties building with the clang provided by the Xcode Command Line Tools, you may want to try installing llvm and using the included clang. Also, some of the dependencies built by `vcpkg` require GNU bison to be installed:
```sh
brew install bison cmake llvm
export CXX=/opt/homebrew/opt/llvm/bin/clang++
```

If you are building against the `main` branch of DuckDB, note that Airport relies on the `httpfs` extension for HTTPS support. Although it builds `httpfs`, it doesn't link it automatically. As a result, during development, you'll need to manually copy the built `httpfs` extension into your local DuckDB extension directory—usually `~/.duckdb/extensions/`.

The following script will copy the necessary extensions to the correct location:

```sh
#!/bin/sh
platform=$(duckdb -noheader -csv -c "pragma platform")
snapshot=$(basename ./build/debug/repository/*)
mkdir -p ~/.duckdb/extensions/$snapshot/$platform/
cp -r ./build/debug/repository/$snapshot ~/.duckdb/extensions/$snapshot
```

## Running the tests
The primary way of testing this extension is the SQL tests in `./test/sql`, run with:

```sh
make test
```
