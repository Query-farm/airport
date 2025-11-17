#include "airport_extension.hpp"
#include "duckdb.hpp"

// Arrow includes.
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/buffer.h>

#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "airport_take_flight.hpp"
#include "airport_json_common.hpp"
#include "airport_json_serializer.hpp"
#include "airport_flight_stream.hpp"
#include "airport_macros.hpp"
#include "airport_request_headers.hpp"
#include "airport_flight_exception.hpp"
#include "duckdb/common/arrow/schema_metadata.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "msgpack.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "storage/airport_catalog.hpp"
#include "airport_rpc.hpp"
#include "airport_schema_utils.hpp"

namespace duckdb
{

  namespace
  {
    struct AirportGetFlightColumnStatistics
    {
      std::string flight_descriptor;
      std::string column_name;
      std::string type;

      MSGPACK_DEFINE_MAP(flight_descriptor, column_name, type)
    };
  }

  unique_ptr<BaseStatistics> AirportTakeFlightStatistics(ClientContext &context, const FunctionData *bind_data, const column_t column_index)
  {
    auto &data = bind_data->Cast<AirportTakeFlightBindData>();

    // printf("Requesting statistics for column %llu\n", column_index);

    if (column_index == COLUMN_IDENTIFIER_ROW_ID || column_index == COLUMN_IDENTIFIER_EMPTY)
    {
      // Don't return unknown statistics for these columns because
      // if you do, it is assumed that DuckDB can get the name of the
      // column.
      return nullptr;
    }

    // So we need to map the column id to the logical type.
    auto &schema = data.schema_root.arrow_schema.children[column_index];

    auto &config = DBConfig::GetConfig(context);

    // printf("Column name is %s\n", schema->name);
    auto arrow_type = AirportGetArrowType(config, *schema);
    auto duck_type = arrow_type->GetDuckType();

    // Talk to the server to get the statistics for the column.
    // this will be done with a do_action call.
    // if it fails just assume the server doesn't implement statistics.
    if (!data.schema_root.arrow_schema.metadata)
    {
      // printf("No metadata attached to flight schema, so cannot produce stats\n");
      return make_uniq<BaseStatistics>(BaseStatistics::CreateUnknown(duck_type));
    }

    auto metadata = ArrowSchemaMetadata(data.schema_root.arrow_schema.metadata);
    auto stats = metadata.GetOption("can_produce_statistics");
    if (stats.empty())
    {
      // printf("Cannot produce stats for this column\n");
      return make_uniq<BaseStatistics>(BaseStatistics::CreateUnknown(duck_type));
    }

    // printf("Can produce statistics for this flight\n");

    arrow::flight::FlightCallOptions call_options;
    airport_add_normal_headers(call_options, data.take_flight_params(), data.trace_id());

    auto &server_location = data.take_flight_params().server_location();

    auto descriptor = AirportLocationDescriptor(server_location, data.descriptor());

    AirportGetFlightColumnStatistics params;
    AIRPORT_ASSIGN_OR_RAISE_CONTAINER(
        params.flight_descriptor,
        data.descriptor().SerializeToString(),
        &descriptor,
        "take_flight_statistics");
    params.column_name = schema->name;
    params.type = duck_type.ToString();

    auto flight_client = AirportAPI::FlightClientForLocation(server_location);
    call_options.headers.emplace_back("airport-action-name", "column_statistics");
    AIRPORT_MSGPACK_ACTION_SINGLE_PARAMETER(action, "column_statistics", params);

    auto stats_buffer = AirportCallAction(flight_client, call_options, action, server_location);

    auto reader = std::make_unique<arrow::io::BufferReader>(stats_buffer->body);

    AIRPORT_ASSIGN_OR_RAISE_CONTAINER(
        auto stats_reader,
        arrow::ipc::RecordBatchStreamReader::Open(std::move(reader)),
        &descriptor, "");

    // Load it into a DataChunk so that all of the types will be easier to work with.

    auto const stats_schema = stats_reader->schema();

    ArrowSchemaWrapper schema_root;

    AIRPORT_ARROW_ASSERT_OK_CONTAINER(
        ExportSchema(*stats_schema, &schema_root.arrow_schema),
        &descriptor,
        "ExportSchema");

    vector<LogicalType> all_types;
    AirportArrowTableSchema arrow_table;
    std::unordered_map<std::string, idx_t> name_indexes;

    for (idx_t col_index = 0; col_index < schema_root.arrow_schema.n_children; col_index++)
    {
      auto &schema_item = *schema_root.arrow_schema.children[col_index];

      auto arrow_type = AirportGetArrowType(config, schema_item);

      all_types.push_back(arrow_type->GetDuckType());
      arrow_table.AddColumn(col_index, std::move(arrow_type), schema_item.name);
      name_indexes[schema_item.name] = col_index;
    }

    AIRPORT_ASSIGN_OR_RAISE_CONTAINER(
        std::shared_ptr<arrow::RecordBatch> batch,
        stats_reader->Next(),
        &descriptor,
        "Failed to read batch from statistics arrow table");

    ArrowSchema c_schema;

    auto current_chunk = make_uniq<ArrowArrayWrapper>();

    AIRPORT_ARROW_ASSERT_OK_CONTAINER(
        arrow::ExportRecordBatch(*batch, &current_chunk->arrow_array, &c_schema),
        &descriptor,
        "Failed to export record batch from statistics arrow table");

    // Extract the values.
    DataChunk stats_chunk;
    stats_chunk.Initialize(Allocator::Get(context),
                           all_types,
                           current_chunk->arrow_array.length);

    stats_chunk.SetCardinality(current_chunk->arrow_array.length);

    D_ASSERT(current_chunk->arrow_array.length == 1);

    ArrowScanLocalState fake_local_state(
        std::move(current_chunk),
        context);

    ArrowTableFunction::ArrowToDuckDB(fake_local_state,
                                      arrow_table.GetColumns(),
                                      stats_chunk,
                                      false);

    stats_chunk.Verify();

    const auto required_fields = {"min", "max", "has_not_null", "has_null", "distinct_count"};
    for (const auto &field : required_fields)
    {
      if (name_indexes.find(field) == name_indexes.end())
      {
        throw InvalidInputException("Statistics for column %s are missing required field %s", schema->name, field);
      }
    }

    switch (duck_type.id())
    {
    case LogicalTypeId::VARCHAR:
    case LogicalTypeId::BLOB:
    {
      auto r = StringStats::CreateEmpty(LogicalType(duck_type.id()));

      StringStats::Update(r, stats_chunk.data[name_indexes["min"]].GetValue(0).GetValue<string>());
      StringStats::Update(r, stats_chunk.data[name_indexes["max"]].GetValue(0).GetValue<string>());
      if (stats_chunk.data[name_indexes["has_not_null"]].GetValue(0).GetValue<bool>())
      {
        r.SetHasNoNull();
      }
      if (stats_chunk.data[name_indexes["has_null"]].GetValue(0).GetValue<bool>())
      {
        r.SetHasNull();
      }
      r.SetDistinctCount(stats_chunk.data[name_indexes["distinct_count"]].GetValue(0).GetValue<int64_t>());

      if (name_indexes.find("contains_unicode") != name_indexes.end())
      {
        if (stats_chunk.data[name_indexes["contains_unicode"]].GetValue(0).GetValue<bool>())
        {
          StringStats::SetContainsUnicode(r);
        }
      }

      if (name_indexes.find("max_string_length") != name_indexes.end())
      {
        StringStats::SetMaxStringLength(r, stats_chunk.data[name_indexes["max_string_length"]].GetValue(0).GetValue<uint64_t>());
      }
      else
      {
        // If the max string length is not present, we assume it is unknown.
        StringStats::ResetMaxStringLength(r);
      }

      return r.ToUnique();
    }
    case LogicalTypeId::UUID:
    case LogicalTypeId::BOOLEAN:
    case LogicalTypeId::TINYINT:
    case LogicalTypeId::SMALLINT:
    case LogicalTypeId::INTEGER:
    case LogicalTypeId::BIGINT:
    case LogicalTypeId::UTINYINT:
    case LogicalTypeId::USMALLINT:
    case LogicalTypeId::UINTEGER:
    case LogicalTypeId::UBIGINT:
    case LogicalTypeId::HUGEINT:
    case LogicalTypeId::UHUGEINT:
    case LogicalTypeId::FLOAT:
    case LogicalTypeId::DOUBLE:
    case LogicalTypeId::TIMESTAMP_TZ:
    case LogicalTypeId::TIMESTAMP:
    case LogicalTypeId::TIMESTAMP_MS:
    case LogicalTypeId::TIMESTAMP_SEC:
    case LogicalTypeId::TIMESTAMP_NS:
    case LogicalTypeId::DATE:
    case LogicalTypeId::TIME:
    case LogicalTypeId::TIME_TZ:
    case LogicalTypeId::TIME_NS:
    case LogicalTypeId::DECIMAL:
    case LogicalTypeId::BIGNUM:

    {
      auto r = NumericStats::CreateEmpty(duck_type);

      NumericStats::SetMin(r, stats_chunk.data[name_indexes["min"]].GetValue(0));
      NumericStats::SetMax(r, stats_chunk.data[name_indexes["max"]].GetValue(0));
      if (stats_chunk.data[name_indexes["has_not_null"]].GetValue(0).GetValue<bool>())
      {
        r.SetHasNoNull();
      }
      if (stats_chunk.data[name_indexes["has_null"]].GetValue(0).GetValue<bool>())
      {
        r.SetHasNull();
      }
      r.SetDistinctCount(stats_chunk.data[name_indexes["distinct_count"]].GetValue(0).GetValue<idx_t>());

      return r.ToUnique();
    }
    case LogicalTypeId::ARRAY:
    case LogicalTypeId::LIST:
    case LogicalTypeId::MAP:
    case LogicalTypeId::STRUCT:
      return make_uniq<BaseStatistics>(BaseStatistics::CreateUnknown(duck_type));

    default:
      throw NotImplementedException("Statistics for type %s not implemented", duck_type.ToString());
    }
  }
}