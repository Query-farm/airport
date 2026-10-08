"""Flight integration regressions, using the built CLI and private loopback servers.

AIRPORT_DUCKDB=/path/to/build/debug/duckdb python -m pytest -q test/python
Dependencies: pytest, pyarrow, msgpack, query-farm-airport-test-server==0.1.1.
"""

import base64
import json
import os
from pathlib import Path
import subprocess
import threading
import uuid

import msgpack
import pyarrow as pa
import pyarrow.flight as flight
import pyarrow.parquet as parquet
import pytest


@pytest.fixture(scope="module")
def duckdb_cli():
    binary = Path(os.environ.get("AIRPORT_DUCKDB", "build/debug/duckdb")).resolve()
    assert binary.is_file(), f"Build DuckDB first or set AIRPORT_DUCKDB: {binary}"
    return binary


def run_sql(binary, sql, *, setup="", success=True):
    # Use a subprocess so a crash is a test failure, and a deadline catches
    # protocol deadlocks (notably at an exact STANDARD_VECTOR_SIZE boundary).
    extension = os.environ.get("AIRPORT_EXTENSION", "airport").replace("'", "''")
    httpfs = binary.parent / "extension/httpfs/httpfs.duckdb_extension"
    load_httpfs = "LOAD '" + str(httpfs).replace("'", "''") + "';" if httpfs.is_file() else ""
    script = f".bail on\n.output /dev/null\n{load_httpfs}\nLOAD '{extension}';\n{setup}\n.output stdout\n{sql}\n"
    result = subprocess.run(
        [str(binary), "-unsigned", "-init", "/dev/null", "-json", "-batch", ":memory:"],
        input=script, text=True, capture_output=True, timeout=60,
        env={**os.environ, "QUERY_FARM_TELEMETRY_OPT_OUT": "1"},
    )
    assert "Sanitizer:" not in result.stderr and "runtime error:" not in result.stderr, result.stderr
    if success:
        assert result.returncode == 0, result.stderr
        return json.loads(result.stdout) if result.stdout.strip() else []
    assert result.returncode == 1, (result.returncode, result.stderr)
    return result.stderr


class LocalEndpointServer(flight.FlightServerBase):
    """Return a local DuckDB function call rather than a remote DoGet stream."""

    def __init__(self, function, arguments, schema):
        super().__init__("grpc://127.0.0.1:0")
        self.schema = schema
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, arguments.schema) as writer:
            writer.write_table(arguments)
        payload = msgpack.packb({"function_name": function, "data": sink.getvalue().to_pybytes()})
        location = "data:application/x-msgpack-duckdb-function-call;base64," + base64.b64encode(payload).decode()
        self.endpoint = flight.FlightEndpoint(b"local", [location])

    def get_flight_info(self, context, descriptor):
        return flight.FlightInfo(self.schema, descriptor, [self.endpoint], -1, -1)

    def do_action(self, context, action):
        assert action.type == "endpoints"
        yield msgpack.packb([self.endpoint.serialize()])


@pytest.mark.parametrize("function", ["range", "generate_series"])
def test_unsupported_local_in_out_is_an_error(duckdb_cli, function):
    arguments = pa.table({"arg_0": pa.array([5], type=pa.int64())})
    with LocalEndpointServer(function, arguments, pa.schema([(function, pa.int64())])) as server:
        error = run_sql(
            duckdb_cli,
            f"SELECT * FROM airport_take_flight('grpc://127.0.0.1:{server.port}', 'local');",
            success=False,
        )
    assert "does not support the scan API" in error
    assert function in error


def test_local_scan_without_local_init(duckdb_cli):
    # repeat has init_global and a scan callback, but no init_local.
    arguments = pa.table({"arg_0": ["hello"], "arg_1": pa.array([4097], type=pa.int64())})
    with LocalEndpointServer("repeat", arguments, pa.schema([("hello", pa.string())])) as server:
        rows = run_sql(
            duckdb_cli,
            f"SELECT count(*) AS n, min(hello) AS value FROM airport_take_flight('grpc://127.0.0.1:{server.port}', 'local');",
        )
    assert rows == [{"n": 4097, "value": "hello"}]


def test_local_parquet_scan_keeps_column_mapping(duckdb_cli, tmp_path):
    source = tmp_path / "data.parquet"
    parquet.write_table(pa.table({"left_col": [1, 2], "right_col": [11, 12]}), source)
    arguments = pa.table({"arg_0": [str(source)]})
    schema = pa.schema([("right_col", pa.int64()), ("left_col", pa.int64())])
    with LocalEndpointServer("parquet_scan", arguments, schema) as server:
        rows = run_sql(duckdb_cli, f"""
SELECT right_col, left_col FROM airport_take_flight('grpc://127.0.0.1:{server.port}', 'local') ORDER BY left_col;
""", setup="LOAD parquet;")
    assert rows == [{"right_col": 11, "left_col": 1}, {"right_col": 12, "left_col": 2}]


def test_local_parquet_multiple_files_and_batches(duckdb_cli, tmp_path):
    paths = []
    count = 4097
    for file_index in range(2):
        source = tmp_path / f"data-{file_index}.parquet"
        values = list(range(file_index * count, (file_index + 1) * count))
        parquet.write_table(pa.table({"left_col": values, "right_col": [v + 10 for v in values]}), source)
        paths.append(str(source))
    arguments = pa.table({"arg_0": [paths], "union_by_name": [True]})
    schema = pa.schema([("right_col", pa.int64()), ("left_col", pa.int64())])
    with LocalEndpointServer("read_parquet", arguments, schema) as server:
        rows = run_sql(duckdb_cli, f"""
SELECT count(*) AS n, sum(left_col)::BIGINT AS total, min(right_col) AS first_value,
       max(right_col) AS last_value
FROM airport_take_flight('grpc://127.0.0.1:{server.port}', 'local');
""", setup="LOAD parquet;")
    assert rows == [{
        "n": count * 2, "total": count * (count * 2 - 1),
        "first_value": 10, "last_value": count * 2 + 9,
    }]


@pytest.fixture(scope="module")
def catalog_server(tmp_path_factory):
    from query_farm_airport_test_server.database_impl import DatabaseLibrary, TableFunction, util_schema
    from query_farm_airport_test_server.server import InMemoryArrowFlightServer
    from query_farm_flight_server import auth, auth_manager_naive, middleware

    def empty_batch(schema):
        return pa.RecordBatch.from_arrays([pa.array([], type=f.type) for f in schema], schema=schema)

    def echo(parameters, output_schema):
        result = empty_batch(output_schema)
        while True:
            chunk = yield (result, True)
            if chunk is None:
                return
            result = chunk

    # The published test server's echo helper predates the flow-control tuple
    # protocol. Supply a current helper in this private test inventory.
    original_echo = util_schema.table_functions_by_name["test_table_in_out_echo"]
    util_schema.table_functions_by_name["test_table_in_out_echo"] = TableFunction(
        input_schema=original_echo.input_schema,
        output_schema_source=original_echo.output_schema_source,
        handler=echo,
    )

    observed = threading.Event()
    input_rows = {"limit": 0}

    def gated_echo(parameters, output_schema):
        result = empty_batch(output_schema)
        batches = 0
        while True:
            chunk = yield (result, True)
            if chunk is None:
                return
            batches += 1
            # The downstream exchange must receive the first output before
            # this exchange can consume more input. Buffering results fails.
            if batches > 1 and not observed.wait(5):
                raise flight.FlightServerError("first output was buffered until input completed")
            result = chunk

    def observe_echo(parameters, output_schema):
        result = empty_batch(output_schema)
        while True:
            chunk = yield (result, True)
            if chunk is None:
                return
            observed.set()
            result = chunk

    def limited_echo(parameters, output_schema):
        result = empty_batch(output_schema)
        input_rows["limit"] = 0
        while True:
            chunk = yield (result, True)
            if chunk is None:
                return
            input_rows["limit"] += len(chunk)
            if input_rows["limit"] > 8192:
                raise flight.FlightServerError("LIMIT did not stop the exchange input")
            result = chunk

    streaming_functions = {
        "test_streaming_gate": gated_echo,
        "test_streaming_observer": observe_echo,
        "test_streaming_limit": limited_echo,
    }
    for name, handler in streaming_functions.items():
        util_schema.table_functions_by_name[name] = TableFunction(
            input_schema=original_echo.input_schema,
            output_schema_source=original_echo.output_schema_source,
            handler=handler,
        )

    def final_batches(parameters, output_schema):
        received = 0
        while True:
            chunk = yield (empty_batch(output_schema), True)
            if chunk is None:
                break
            received += len(chunk)
        count = parameters.parameters.column(0)[0].as_py()
        return [pa.RecordBatch.from_arrays([pa.array([received] * count, type=pa.int64())], schema=output_schema)]

    util_schema.table_functions_by_name["test_final_batches"] = TableFunction(
        input_schema=pa.schema([
            pa.field("count", pa.int64()),
            pa.field("table_input", pa.string(), metadata={"is_table_type": "1"}),
        ]),
        output_schema_source=pa.schema([("received", pa.int64())]),
        handler=final_batches,
    )

    class CaptureServer(InMemoryArrowFlightServer):
        def do_action(self, context, action):
            if action.type == "add_field":
                self.add_field = msgpack.unpackb(action.body.to_pybytes(), raw=False, unicode_errors="surrogateescape")
                # Capture the wire request before the test server's unsupported
                # nested-field mutation. The client must surface a normal error.
                raise flight.FlightServerError("captured add_field request")
            return super().do_action(context, action)

    state_dir = tmp_path_factory.mktemp("airport-catalog")
    original_filename = DatabaseLibrary.filename_for_token
    DatabaseLibrary.filename_for_token = staticmethod(lambda token: str(state_dir / f"{token}.pkl"))
    manager = auth_manager_naive.AuthManagerNaive(
        account_type=auth.Account, token_type=auth.AccountToken, allow_anonymous_access=False,
    )
    server = CaptureServer(
        location="grpc://127.0.0.1:0", auth_manager=manager,
        middleware={
            "headers": middleware.SaveHeadersMiddlewareFactory(),
            "auth": middleware.AuthManagerMiddlewareFactory(auth_manager=manager),
        },
    )
    server.streaming_observed = observed
    server.streaming_input_rows = input_rows
    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        thread.join(timeout=10)
        DatabaseLibrary.filename_for_token = staticmethod(original_filename)
        del util_schema.table_functions_by_name["test_final_batches"]
        for name in streaming_functions:
            del util_schema.table_functions_by_name[name]
        util_schema.table_functions_by_name["test_table_in_out_echo"] = original_echo


def catalog_setup(server):
    location = f"grpc://127.0.0.1:{server.port}"
    return f"""
CREATE SECRET (TYPE airport, auth_token '{uuid.uuid4()}', SCOPE '{location}');
CALL airport_action('{location}', 'create_database', 'test1');
ATTACH 'test1' (TYPE airport, location '{location}');
SET threads=8;
"""


@pytest.mark.parametrize("disabled", [False, True])
def test_union_all_finishes_once(duckdb_cli, catalog_server, disabled):
    setup = catalog_setup(catalog_server) + "CREATE TEMP TABLE words AS SELECT 'hello' AS txt UNION ALL SELECT 'world';"
    if disabled:
        setup += "PRAGMA disable_optimizer;"
    rows = run_sql(duckdb_cli, """
SELECT * FROM test1.utils.test_table_in_out('Sloane', (
    SELECT txt FROM words UNION ALL SELECT 'again'
)) ORDER BY 1, 2;
""", setup=setup)
    assert [list(row.values()) for row in rows] == [
        ["Sloane", "again"], ["Sloane", "hello"], ["Sloane", "world"], ["last", "row"],
    ]


@pytest.mark.parametrize("count", [0, 1, 2048, 2049, 10000])
def test_echo_batch_boundaries(duckdb_cli, catalog_server, count):
    rows = run_sql(duckdb_cli, f"""
SELECT count(*) AS n, sum(i)::BIGINT AS total FROM test1.utils.test_table_in_out_echo((
    SELECT i FROM range({count}) t(i) UNION ALL SELECT {count}::BIGINT
));
""", setup=catalog_setup(catalog_server))
    assert rows == [{"n": count + 1, "total": count * (count + 1) // 2}]


def test_empty_input_still_finalizes(duckdb_cli, catalog_server):
    rows = run_sql(duckdb_cli, """
SELECT * FROM test1.utils.test_table_in_out('Sloane', (
    SELECT ''::VARCHAR AS txt WHERE false
));
""", setup=catalog_setup(catalog_server))
    assert [list(row.values()) for row in rows] == [["last", "row"]]


@pytest.mark.parametrize("disabled", [False, True])
def test_output_streams_before_input_finishes(duckdb_cli, catalog_server, disabled):
    catalog_server.streaming_observed.clear()
    setup = catalog_setup(catalog_server)
    if disabled:
        setup += "PRAGMA disable_optimizer;"
    rows = run_sql(duckdb_cli, """
SELECT count(*) AS n, sum(i)::BIGINT AS total
FROM test1.utils.test_streaming_observer((
    SELECT * FROM test1.utils.test_streaming_gate((
        SELECT i FROM range(8193) t(i) UNION ALL SELECT 8193
    ))
));
""", setup=setup)
    assert rows == [{"n": 8194, "total": 8193 * 8194 // 2}]
    assert catalog_server.streaming_observed.is_set()


@pytest.mark.parametrize("disabled", [False, True])
@pytest.mark.parametrize("union", [False, True])
def test_limit_stops_input_early(duckdb_cli, catalog_server, disabled, union):
    setup = catalog_setup(catalog_server)
    if disabled:
        setup += "PRAGMA disable_optimizer;"
    second_branch = "UNION ALL SELECT 100000" if union else ""
    rows = run_sql(duckdb_cli, f"""
SELECT * FROM test1.utils.test_streaming_limit((
    SELECT i FROM range(100000) t(i) {second_branch}
)) LIMIT 1;
""", setup=setup)
    assert rows == [{"i": 0}]
    assert 0 < catalog_server.streaming_input_rows["limit"] <= 8192


def test_limit_can_abandon_large_response(duckdb_cli, catalog_server):
    rows = run_sql(duckdb_cli, """
SELECT * FROM test1.utils.test_table_in_out_long((SELECT i + 7 AS i FROM range(2048) t(i))) LIMIT 1;
""", setup=catalog_setup(catalog_server))
    assert rows == [{"i": 7}]


def test_limit_stops_chained_exchanges(duckdb_cli, catalog_server):
    rows = run_sql(duckdb_cli, """
SELECT * FROM test1.utils.test_streaming_observer((
    SELECT * FROM test1.utils.test_streaming_limit((SELECT i FROM range(100000) t(i)))
)) LIMIT 1;
""", setup=catalog_setup(catalog_server))
    assert rows == [{"i": 0}]
    assert 0 < catalog_server.streaming_input_rows["limit"] <= 8192


def test_repeated_prepared_execution(duckdb_cli, catalog_server):
    setup = catalog_setup(catalog_server) + """
PREPARE exchange AS SELECT count(*) AS n FROM test1.utils.test_table_in_out_echo((
    SELECT i FROM range(4097) t(i) UNION ALL SELECT 9999
));
EXECUTE exchange;
"""
    assert run_sql(duckdb_cli, "EXECUTE exchange;", setup=setup) == [{"n": 4098}]


def test_multiple_output_batches(duckdb_cli, catalog_server):
    rows = run_sql(duckdb_cli, """
SELECT count(*) AS n FROM test1.utils.test_table_in_out_long((
    SELECT i FROM range(2048) t(i) UNION ALL SELECT 9999
));
""", setup=catalog_setup(catalog_server))
    assert rows == [{"n": 20490}]


@pytest.mark.parametrize("count", [2048, 4096, 4097])
def test_final_output_batch_boundaries(duckdb_cli, catalog_server, count):
    rows = run_sql(duckdb_cli, f"""
SELECT count(*) AS n, min(received) AS received FROM test1.utils.test_final_batches({count}, (
    SELECT i FROM range(5000) t(i) UNION ALL SELECT 9999
));
""", setup=catalog_setup(catalog_server))
    assert rows == [{"n": count, "received": 5001}]


def test_cte_used_twice(duckdb_cli, catalog_server):
    rows = run_sql(duckdb_cli, """
WITH results AS NOT MATERIALIZED (
    SELECT * FROM test1.utils.test_table_in_out_echo((SELECT 1 AS i UNION ALL SELECT 2))
)
SELECT sum(i)::BIGINT AS total FROM (SELECT * FROM results UNION ALL SELECT * FROM results);
""", setup=catalog_setup(catalog_server))
    assert rows == [{"total": 6}]


def test_projection_keeps_all_server_input_columns(duckdb_cli, catalog_server):
    rows = run_sql(duckdb_cli, """
SELECT result_8, result_19 FROM test1.utils.test_table_in_out_wide('hello', (
    SELECT 1 AS i, 'unused locally' AS txt UNION ALL SELECT 2, 'still sent'
));
""", setup=catalog_setup(catalog_server))
    assert rows == [{"result_8": 8, "result_19": 19}] * 2


@pytest.mark.parametrize("path", ["data_struct", "data_struct.nested"])
def test_add_field_uses_leaf_name(duckdb_cli, catalog_server, path):
    setup = catalog_setup(catalog_server) + """
CREATE SCHEMA test1.main;
CREATE TABLE test1.main.nested_fields (data_struct STRUCT(existing INTEGER, nested STRUCT(existing INTEGER)));
"""
    error = run_sql(
        duckdb_cli, f'ALTER TABLE test1.main.nested_fields ADD COLUMN {path}."new field" VARCHAR;',
        setup=setup, success=False,
    )
    assert "captured add_field request" in error
    request = catalog_server.add_field
    assert request["column_path"] == path.split(".")
    schema_bytes = request["column_schema"]
    if isinstance(schema_bytes, str):
        schema_bytes = schema_bytes.encode("utf-8", errors="surrogateescape")
    schema = pa.ipc.read_schema(pa.BufferReader(schema_bytes))
    assert schema.names == ["new field"]
    assert schema.field(0).type == pa.string()
