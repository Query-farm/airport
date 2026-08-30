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
