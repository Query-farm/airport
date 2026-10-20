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

The contributor regression tests start private Flight servers on loopback ports.
They exercise endpoint callback errors, nested-field action serialization, and
table-input exchanges across multiple input pipelines. Run them against the CLI
from the build being tested:

```sh
uv venv --python 3.12 .test-venv
uv pip install --python .test-venv/bin/python pytest pyarrow msgpack query-farm-airport-test-server==0.1.1
AIRPORT_DUCKDB="$PWD/build/debug/duckdb" .test-venv/bin/python -m pytest -q test/python
```

Most SQL tests also require an Airport test server. With one running on a local
port, refresh the installed Airport binary after rebuilding, then run the debug
suite with the built extensions available to the test runner:

```sh
./build/debug/duckdb -unsigned -c "FORCE INSTALL airport FROM '$PWD/build/debug/repository';"
AIRPORT_TEST_SERVER=grpc://127.0.0.1:8815 \
DUCKDB_TEST_AUTOLOADING=all \
DUCKDB_TEST_STATICALLY_LOADED_EXTENSIONS='["core_functions","parquet","airport"]' \
make test_debug
```

Check the test summary for skipped requirements: missing extensions or an unset
`AIRPORT_TEST_SERVER` can otherwise cause integration tests to be skipped.

Table-input exchanges stream responses directly through the input pipeline. A
dependent source pipeline closes the Flight writer and streams its final output
after all input pipelines finish, including every `UNION ALL` branch. A downstream
`LIMIT` can stop the input early and cancel the exchange; Airport does not
materialize the input or results.

Local endpoint delegation supports table functions with the scan API. Functions
that only implement the in/out API, including `range` and `generate_series`,
produce a `NotImplementedException` rather than crashing the client.

For the `add_field` action, `column_path` identifies the parent struct (including
any nested parents); the single field in `column_schema` contains the new leaf
field's name and type.
