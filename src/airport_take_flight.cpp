#include "airport_extension.hpp"
#include "duckdb.hpp"

// Arrow includes.
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/buffer.h>

#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "airport_take_flight.hpp"
#include "airport_json_common.hpp"
#include "airport_json_serializer.hpp"
#include "airport_flight_stream.hpp"
#include "airport_macros.hpp"
#include "airport_secrets.hpp"
#include "airport_request_headers.hpp"
#include "airport_flight_exception.hpp"
#include "duckdb/common/arrow/schema_metadata.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "msgpack.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"

namespace duckdb
{

  static std::string join_vector_of_strings(const std::vector<std::string> &vec, const char joiner)
  {
    if (vec.empty())
      return "";

    return std::accumulate(
        std::next(vec.begin()), vec.end(), vec.front(),
        [joiner](const std::string &a, const std::string &b)
        {
          return a + joiner + b;
        });
  }

  template <typename T>
  static std::vector<std::string> convert_to_strings(const std::vector<T> &vec)
  {
    std::vector<std::string> result(vec.size());
    std::transform(vec.begin(), vec.end(), result.begin(), [](const T &elem)
                   { return std::to_string(elem); });
    return result;
  }

  // Create a FlightDescriptor from a DuckDB value which can be one of a few different
  // types.
  static flight::FlightDescriptor flight_descriptor_from_value(duckdb::Value &flight_descriptor)
  {
    switch (flight_descriptor.type().id())
    {
    case LogicalTypeId::BLOB:
    case LogicalTypeId::VARCHAR:
      return flight::FlightDescriptor::Command(flight_descriptor.ToString());
    case LogicalTypeId::LIST:
    {
      auto &list_values = ListValue::GetChildren(flight_descriptor);
      vector<string> components;
      for (idx_t i = 0; i < list_values.size(); i++)
      {
        auto &child = list_values[i];
        if (child.type().id() != LogicalTypeId::VARCHAR)
        {
          throw InvalidInputException("airport_take_flight: when specifying a path all list components must be a varchar");
        }
        components.emplace_back(child.ToString());
      }
      return flight::FlightDescriptor::Path(components);
    }
    case LogicalTypeId::ARRAY:
    {
      auto &array_values = ArrayValue::GetChildren(flight_descriptor);
      vector<string> components;
      for (idx_t i = 0; i < array_values.size(); i++)
      {
        auto &child = array_values[i];
        if (child.type().id() != LogicalTypeId::VARCHAR)
        {
          throw InvalidInputException("airport_take_flight: when specifying a path all list components must be a varchar");
        }
        components.emplace_back(child.ToString());
      }
      return flight::FlightDescriptor::Path(components);
    }
    // FIXME: deal with the union type returned by Arrow list flights.
    default:
      throw InvalidInputException("airport_take_flight: unknown descriptor type passed");
    }
  }

  AirportTakeFlightParameters AirportParseTakeFlightParameters(
      string &server_location,
      ClientContext &context,
      TableFunctionBindInput &input)
  {
    AirportTakeFlightParameters params;

    params.server_location = server_location;

    for (auto &kv : input.named_parameters)
    {
      auto loption = StringUtil::Lower(kv.first);
      if (loption == "auth_token")
      {
        params.auth_token = StringValue::Get(kv.second);
      }
      else if (loption == "secret")
      {
        params.secret_name = StringValue::Get(kv.second);
      }
      else if (loption == "ticket")
      {
        params.ticket = StringValue::Get(kv.second);
      }
      else if (loption == "headers")
      {
        // Now we need to parse out the map contents.
        auto &children = duckdb::MapValue::GetChildren(kv.second);

        for (auto &value_pair : children)
        {
          auto &child_struct = duckdb::StructValue::GetChildren(value_pair);
          auto key = StringValue::Get(child_struct[0]);
          auto value = StringValue::Get(child_struct[1]);

          params.user_supplied_headers[key].push_back(value);
        }
      }
    }

    params.auth_token = AirportAuthTokenForLocation(context, params.server_location, params.secret_name, params.auth_token);
    return params;
  }

  unique_ptr<FunctionData>
  AirportTakeFlightBindWithFlightDescriptor(
      const AirportTakeFlightParameters &take_flight_params,
      const flight::FlightDescriptor &descriptor,
      ClientContext &context,
      TableFunctionBindInput &input,
      vector<LogicalType> &return_types,
      vector<string> &names,
      std::shared_ptr<flight::FlightInfo> *cached_info_ptr,
      std::shared_ptr<GetFlightInfoTableFunctionParameters> table_function_parameters)
  {

    // Create a UID for tracing.
    auto trace_uuid = UUID::ToString(UUID::GenerateRandomUUID());

    D_ASSERT(!take_flight_params.server_location.empty());

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(auto location, flight::Location::Parse(take_flight_params.server_location),
                                                       take_flight_params.server_location,
                                                       descriptor,
                                                       "");

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(auto flight_client,
                                                       flight::FlightClient::Connect(location),
                                                       take_flight_params.server_location,
                                                       descriptor,
                                                       "");

    arrow::flight::FlightCallOptions call_options;
    airport_add_standard_headers(call_options, take_flight_params.server_location);
    airport_add_authorization_header(call_options, take_flight_params.auth_token);

    call_options.headers.emplace_back("airport-trace-id", trace_uuid);

    if (descriptor.type == arrow::flight::FlightDescriptor::PATH)
    {
      auto path_parts = descriptor.path;
      std::string joined_path_parts = join_vector_of_strings(path_parts, '/');
      call_options.headers.emplace_back("airport-flight-path", joined_path_parts);
    }

    for (const auto &header_pair : take_flight_params.user_supplied_headers)
    {
      for (const auto &header_value : header_pair.second)
      {
        call_options.headers.emplace_back(header_pair.first, header_value);
      }
    }

    // Get the information about the flight, this will allow the
    // endpoint information to be returned.
    unique_ptr<AirportTakeFlightScanData> scan_data;
    if (cached_info_ptr != nullptr)
    {
      scan_data = make_uniq<AirportTakeFlightScanData>(
          take_flight_params.server_location,
          *cached_info_ptr,
          nullptr);
    }
    else
    {
      std::unique_ptr<arrow::flight::FlightInfo> retrieved_flight_info;

      if (table_function_parameters != nullptr)
      {
        // Rather than calling GetFlightInfo we will call DoAction and get
        // get the flight info that way, since it allows us to serialize
        // all of the data we need to send instead of just the flight name.
        std::stringstream packed_buffer;
        msgpack::pack(packed_buffer, table_function_parameters);
        arrow::flight::Action action{"get_flight_info_table_function",
                                     arrow::Buffer::FromString(packed_buffer.str())};

        AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto action_results, flight_client->DoAction(call_options, action), take_flight_params.server_location, "airport_get_flight_info_table_function");

        // The only item returned is a serialized flight info.
        AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto serialized_flight_info_buffer, action_results->Next(), take_flight_params.server_location, "reading get_flight_info for table function");

        std::string_view serialized_flight_info(reinterpret_cast<const char *>(serialized_flight_info_buffer->body->data()), serialized_flight_info_buffer->body->size());

        // Now deserialize that flight info so we can use it.
        AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(retrieved_flight_info, arrow::flight::FlightInfo::Deserialize(serialized_flight_info), take_flight_params.server_location, "deserialize flight info");

        AIRPORT_ARROW_ASSERT_OK_LOCATION(action_results->Drain(), take_flight_params.server_location, "");
      }
      else
      {
        AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(retrieved_flight_info,
                                                           flight_client->GetFlightInfo(call_options, descriptor),
                                                           take_flight_params.server_location,
                                                           descriptor,
                                                           "");
      }

      // Assert that the descriptor is the same as the one that was passed in.
      if (descriptor != retrieved_flight_info->descriptor())
      {
        throw InvalidInputException("airport_take_flight: descriptor returned from server does not match the descriptor that was passed in to GetFlightInfo, check with Flight server implementation.");
      }

      scan_data = make_uniq<AirportTakeFlightScanData>(
          take_flight_params.server_location,
          std::move(retrieved_flight_info),
          nullptr);
    }

    // Store the flight info on the bind data.

    // After doing a little bit of examination of the DuckDb sources, I learned that
    // that DuckDb supports the "C" interface of Arrow, this means that DuckDB doens't
    // actually have a dependency on Arrow.
    //
    // Arrow Flight requires a dependency on the full Arrow library, because of all of
    // the dependencies.
    //
    // Thankfully there is a "bridge" interface between the C++ based Arrow types returned
    // by the C++ Arrow library and the C based Arrow types that DuckDB already knows how to
    // consume.
    auto ret = make_uniq<AirportTakeFlightBindData>(
        (stream_factory_produce_t)&AirportFlightStreamReader::CreateStream,
        (uintptr_t)scan_data.get());

    ret->scan_data = std::move(scan_data);
    ret->flight_client = std::move(flight_client);
    ret->ticket = take_flight_params.ticket;
    ret->user_supplied_headers = take_flight_params.user_supplied_headers;
    ret->auth_token = take_flight_params.auth_token;
    ret->server_location = take_flight_params.server_location;
    ret->trace_id = trace_uuid;
    ret->table_function_parameters = table_function_parameters;
    auto &data = *ret;

    // Convert the C++ schema into the C format schema, but store it on the bind
    // information
    std::shared_ptr<arrow::Schema> info_schema;
    arrow::ipc::DictionaryMemo dictionary_memo;
    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(info_schema,
                                                       ret->scan_data->flight_info_->GetSchema(&dictionary_memo),
                                                       take_flight_params.server_location,
                                                       descriptor,
                                                       "");

    AIRPORT_ARROW_ASSERT_OK_LOCATION_DESCRIPTOR(ExportSchema(*info_schema, &data.schema_root.arrow_schema), take_flight_params.server_location, descriptor, "ExportSchema");

    for (idx_t col_idx = 0;
         col_idx < (idx_t)data.schema_root.arrow_schema.n_children; col_idx++)
    {
      auto &schema = *data.schema_root.arrow_schema.children[col_idx];
      if (!schema.release)
      {
        throw InvalidInputException("airport_take_flight: released schema passed");
      }
      auto arrow_type = ArrowType::GetArrowLogicalType(DBConfig::GetConfig(context), schema);

      // Determine if the column is the row_id column by looking at the metadata
      // on the column.
      bool is_row_id_column = false;
      if (schema.metadata != nullptr)
      {
        auto column_metadata = ArrowSchemaMetadata(schema.metadata);

        auto comment = column_metadata.GetOption("is_row_id");
        if (!comment.empty())
        {
          is_row_id_column = true;
          ret->row_id_column_index = col_idx;
        }
      }

      if (schema.dictionary)
      {
        auto dictionary_type = ArrowType::GetArrowLogicalType(DBConfig::GetConfig(context), *schema.dictionary);
        arrow_type->SetDictionary(std::move(dictionary_type));
      }

      if (!is_row_id_column)
      {
        return_types.emplace_back(arrow_type->GetDuckType());
      }

      ret->arrow_table.AddColumn(is_row_id_column ? COLUMN_IDENTIFIER_ROW_ID : col_idx, std::move(arrow_type));

      auto format = string(schema.format);
      auto name = string(schema.name);
      if (name.empty())
      {
        name = string("v") + to_string(col_idx);
      }
      if (!is_row_id_column)
      {
        names.push_back(name);
      }
      // printf("Added column with id %lld %s\n", is_row_id_column ? COLUMN_IDENTIFIER_ROW_ID : col_idx, name.c_str());
    }
    QueryResult::DeduplicateColumns(names);
    return std::move(ret);
  }

  static unique_ptr<FunctionData> take_flight_bind(
      ClientContext &context,
      TableFunctionBindInput &input,
      vector<LogicalType> &return_types,
      vector<string> &names)
  {
    auto server_location = input.inputs[0].ToString();
    auto params = AirportParseTakeFlightParameters(server_location, context, input);
    auto descriptor = flight_descriptor_from_value(input.inputs[1]);

    return AirportTakeFlightBindWithFlightDescriptor(
        params,
        descriptor,
        context,
        input, return_types, names, nullptr, nullptr);
  }

  static unique_ptr<FunctionData> take_flight_bind_with_pointer(
      ClientContext &context,
      TableFunctionBindInput &input,
      vector<LogicalType> &return_types,
      vector<string> &names)
  {
    auto server_location = input.inputs[0].ToString();
    auto params = AirportParseTakeFlightParameters(server_location, context, input);

    if (input.inputs[1].IsNull())
    {
      throw BinderException("airport: take_flight_with_pointer, pointers to flight descriptor cannot be null");
    }

    const auto info = reinterpret_cast<std::shared_ptr<flight::FlightInfo> *>(input.inputs[1].GetPointer());

    return AirportTakeFlightBindWithFlightDescriptor(
        params,
        info->get()->descriptor(),
        context,
        input,
        return_types,
        names,
        info,
        nullptr);
  }

  void AirportTakeFlight(ClientContext &context, TableFunctionInput &data_p, DataChunk &output)
  {
    if (!data_p.local_state)
    {
      return;
    }
    auto &data = data_p.bind_data->CastNoConst<ArrowScanFunctionData>();
    auto &state = data_p.local_state->Cast<ArrowScanLocalState>();
    auto &global_state = data_p.global_state->Cast<ArrowScanGlobalState>();
    auto &airport_bind_data = data_p.bind_data->Cast<AirportTakeFlightBindData>();

    //! Out of tuples in this chunk
    if (state.chunk_offset >= (idx_t)state.chunk->arrow_array.length)
    {
      if (!ArrowTableFunction::ArrowScanParallelStateNext(
              context,
              data_p.bind_data.get(),
              state,
              global_state))
      {
        return;
      }
    }
    int64_t output_size =
        MinValue<int64_t>(STANDARD_VECTOR_SIZE,
                          state.chunk->arrow_array.length - state.chunk_offset);
    data.lines_read += output_size;

    if (global_state.CanRemoveFilterColumns())
    {
      state.all_columns.Reset();
      state.all_columns.SetCardinality(output_size);
      ArrowTableFunction::ArrowToDuckDB(state, data.arrow_table.GetColumns(), state.all_columns,
                                        data.lines_read - output_size, false, airport_bind_data.row_id_column_index);
      output.ReferenceColumns(state.all_columns, global_state.projection_ids);
    }
    else
    {
      output.SetCardinality(output_size);
      ArrowTableFunction::ArrowToDuckDB(state, data.arrow_table.GetColumns(), output,
                                        data.lines_read - output_size, false, airport_bind_data.row_id_column_index);
    }

    output.Verify();
    state.chunk_offset += output.size();
  }

  struct GetFlightColumnStatistics
  {
    std::string flight_descriptor;
    std::string column_name;
    std::string type;

    MSGPACK_DEFINE_MAP(flight_descriptor, column_name, type)
  };

  struct GetFlightColumnStatisticsNumericValue
  {
    bool boolean;
    int8_t tinyint;
    int16_t smallint;
    int32_t integer;
    int64_t bigint;
    uint8_t utinyint;
    uint16_t usmallint;
    uint32_t uinteger;
    uint64_t ubigint;

    uint64_t hugeint_high;
    uint64_t hugeint_low;

    float float_;   // NOLINT
    double double_; // NOLINT

    MSGPACK_DEFINE_MAP(boolean, tinyint, smallint, integer, bigint, utinyint, usmallint, uinteger, ubigint,
                       hugeint_high, hugeint_low,
                       float_, double_)
  };

  Value GetValueForType(LogicalTypeId type, const GetFlightColumnStatisticsNumericValue &v)
  {
    switch (type)
    {
    case LogicalTypeId::BOOLEAN:
      return Value::BOOLEAN(v.boolean);
    case LogicalTypeId::TINYINT:
      return Value::TINYINT(v.tinyint);
    case LogicalTypeId::SMALLINT:
      return Value::SMALLINT(v.smallint);
    case LogicalTypeId::INTEGER:
      return Value::INTEGER(v.integer);
    case LogicalTypeId::BIGINT:
      return Value::BIGINT(v.bigint);
    case LogicalTypeId::UTINYINT:
      return Value::UTINYINT(v.utinyint);
    case LogicalTypeId::USMALLINT:
      return Value::USMALLINT(v.usmallint);
    case LogicalTypeId::UINTEGER:
      return Value::UINTEGER(v.uinteger);
    case LogicalTypeId::UBIGINT:
      return Value::UBIGINT(v.ubigint);
    case LogicalTypeId::TIMESTAMP_TZ:
      return Value::TIMESTAMPTZ(timestamp_tz_t(v.bigint));
    case LogicalTypeId::HUGEINT:
    {
      hugeint_t t = ((hugeint_t)v.hugeint_high << 64) | v.hugeint_low;
      return Value::HUGEINT(t);
    }
    case LogicalTypeId::UHUGEINT:
    {
      uhugeint_t t = ((uhugeint_t)v.hugeint_high << 64) | v.hugeint_low;
      return Value::UHUGEINT(t);
    }
    case LogicalTypeId::FLOAT:
      return Value::FLOAT(v.float_);
    case LogicalTypeId::DOUBLE:
      return Value::DOUBLE(v.double_);
    default:
      throw InvalidInputException("Unknown type passed to GetValueForType");
    }
  }

  struct GetFlightColumnStatisticsNumericStatsData
  {
    //! Whether or not the value has a max value
    bool has_min;
    //! Whether or not the segment has a min value
    bool has_max;
    //! The minimum value of the segment
    GetFlightColumnStatisticsNumericValue min;
    //! The maximum value of the segment
    GetFlightColumnStatisticsNumericValue max;

    MSGPACK_DEFINE_MAP(has_min, has_max, min, max)
  };

  struct GetFlightColumnStatisticsStringData
  {
    std::string min;
    std::string max;
    MSGPACK_DEFINE_MAP(min, max)
  };

  struct GetFlightColumnStatisticsResult
  {
    //! Whether or not the segment can contain NULL values
    bool has_null;
    //! Whether or not the segment can contain values that are not null
    bool has_no_null;
    // estimate that one may have even if distinct_stats==nullptr
    idx_t distinct_count;

    //! Numeric and String stats
    GetFlightColumnStatisticsNumericStatsData numeric_stats;
    GetFlightColumnStatisticsStringData string_stats;

    MSGPACK_DEFINE_MAP(has_null, has_no_null, distinct_count, numeric_stats, string_stats)
  };

  static unique_ptr<BaseStatistics> take_flight_statistics(ClientContext &context, const FunctionData *bind_data, column_t column_index)
  {
    auto &data = bind_data->Cast<AirportTakeFlightBindData>();

    // printf("Requesting statistics for column %llu\n", column_index);

    if (column_index == COLUMN_IDENTIFIER_ROW_ID)
    {
      return make_uniq<BaseStatistics>(BaseStatistics::CreateEmpty(LogicalType::BIGINT));
    }

    // So we need to map the column id to the logical type.
    auto &schema = data.schema_root.arrow_schema.children[column_index];

    // printf("Column name is %s\n", schema->name);
    auto arrow_type = ArrowType::GetArrowLogicalType(DBConfig::GetConfig(context), *schema);
    if (schema->dictionary)
    {
      auto dictionary_type = ArrowType::GetArrowLogicalType(DBConfig::GetConfig(context), *schema->dictionary);
      arrow_type->SetDictionary(std::move(dictionary_type));
    }
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
    airport_add_standard_headers(call_options, data.server_location);
    airport_add_authorization_header(call_options, data.auth_token);

    call_options.headers.emplace_back("airport-trace-id", data.trace_id);

    std::stringstream packed_buffer;

    GetFlightColumnStatistics params;
    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(
        params.flight_descriptor,
        data.scan_data->flight_info_->descriptor().SerializeToString(),
        data.server_location,
        "take_flight_statitics");
    params.column_name = schema->name;
    params.type = duck_type.ToString();

    msgpack::pack(packed_buffer, params);
    arrow::flight::Action action{"get_flight_column_statistics",
                                 arrow::Buffer::FromString(packed_buffer.str())};

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto action_results, data.flight_client->DoAction(call_options, action), data.server_location, "take_flight_statitics");

    // The only item returned is a serialized flight info.
    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto stats_buffer, action_results->Next(), data.server_location, "reading take_flight_statistics for a column");

    std::string_view serialized_column_statistics(reinterpret_cast<const char *>(stats_buffer->body->data()), stats_buffer->body->size());

    // No unpack the serialized result.
    GetFlightColumnStatisticsResult col_stats;
    // Do this to make the memory sanitizer okay with the union
    memset(&col_stats, 0, sizeof(col_stats));

    try
    {
      msgpack::object_handle oh = msgpack::unpack(
          (const char *)stats_buffer->body->data(),
          stats_buffer->body->size(),
          0);
      msgpack::object obj = oh.get();
      obj.convert(col_stats);
    }
    catch (const std::exception &e)
    {
      throw AirportFlightException(data.server_location,
                                   "File to parse msgpack encoded column statistics: " + string(e.what()));
    }

    AIRPORT_ARROW_ASSERT_OK_LOCATION(action_results->Drain(), data.server_location, "");

    switch (duck_type.id())
    {
    case LogicalTypeId::VARCHAR:
    {
      auto result = StringStats::CreateEmpty(LogicalType(duck_type.id()));
      if (col_stats.has_no_null)
      {
        result.SetHasNoNull();
      }
      if (col_stats.has_null)
      {
        result.SetHasNull();
      }
      result.SetDistinctCount(col_stats.distinct_count);
      StringStats::Update(result, col_stats.string_stats.min);
      StringStats::Update(result, col_stats.string_stats.max);
      return result.ToUnique();
    }
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
    {
      auto result = NumericStats::CreateEmpty(LogicalType(duck_type.id()));
      if (col_stats.has_no_null)
      {
        result.SetHasNoNull();
      }
      if (col_stats.has_null)
      {
        result.SetHasNull();
      }
      result.SetDistinctCount(col_stats.distinct_count);
      NumericStats::SetMin(result, GetValueForType(duck_type.id(), col_stats.numeric_stats.min));
      NumericStats::SetMax(result, GetValueForType(duck_type.id(), col_stats.numeric_stats.max));
      return result.ToUnique();
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

  static unique_ptr<NodeStatistics> take_flight_cardinality(ClientContext &context, const FunctionData *data)
  {
    // To estimate the cardinality of the flight, we can peek at the flight information
    // that was retrieved during the bind function.
    //
    // This estimate does not take into account any filters that may have been applied
    //
    auto &bind_data = data->Cast<AirportTakeFlightBindData>();
    auto flight_estimated_records = bind_data.scan_data.get()->total_records();

    if (flight_estimated_records != -1)
    {
      return make_uniq<NodeStatistics>(flight_estimated_records);
    }
    // If we don't have an estimated number of records, just use an assumption.
    return make_uniq<NodeStatistics>(100000);
  }

  static void take_flight_complex_filter_pushdown(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                                  vector<unique_ptr<Expression>> &filters)
  {
    auto allocator = AirportJSONAllocator(BufferAllocator::Get(context));

    auto alc = allocator.GetYYAlc();

    auto doc = AirportJSONCommon::CreateDocument(alc);
    auto result_obj = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, result_obj);

    auto filters_arr = yyjson_mut_arr(doc);

    for (auto &f : filters)
    {
      auto serializer = AirportJsonSerializer(doc, false, false, false);
      f->Serialize(serializer);
      yyjson_mut_arr_append(filters_arr, serializer.GetRootObject());
    }

    yyjson_mut_val *column_id_names = yyjson_mut_arr(doc);
    for (auto id : get.GetColumnIds())
    {
      // So there can be a special column id specified called rowid.
      yyjson_mut_arr_add_str(doc, column_id_names, id.IsRowIdColumn() ? "rowid" : get.names[id.GetPrimaryIndex()].c_str());
    }

    yyjson_mut_obj_add_val(doc, result_obj, "filters", filters_arr);
    yyjson_mut_obj_add_val(doc, result_obj, "column_binding_names_by_index", column_id_names);
    idx_t len;
    auto data = yyjson_mut_val_write_opts(
        result_obj,
        AirportJSONCommon::WRITE_FLAG,
        alc, reinterpret_cast<size_t *>(&len), nullptr);

    if (data == nullptr)
    {
      throw SerializationException(
          "Failed to serialize json, perhaps the query contains invalid utf8 characters?");
    }

    auto json_result = string(data, (size_t)len);

    auto &bind_data = bind_data_p->Cast<AirportTakeFlightBindData>();

    bind_data.json_filters = json_result;
  }

  unique_ptr<ArrowArrayStreamWrapper> AirportProduceArrowScan(const ArrowScanFunctionData &function,
                                                              const vector<column_t> &column_ids, TableFilterSet *filters)
  {
    //! Generate Projection Pushdown Vector
    ArrowStreamParameters parameters;
    for (idx_t idx = 0; idx < column_ids.size(); idx++)
    {
      auto col_idx = column_ids[idx];
      if (col_idx != COLUMN_IDENTIFIER_ROW_ID)
      {
        auto &schema = *function.schema_root.arrow_schema.children[col_idx];
        parameters.projected_columns.projection_map[idx] = schema.name;
        parameters.projected_columns.columns.emplace_back(schema.name);
        parameters.projected_columns.filter_to_col[idx] = col_idx;
      }
    }
    parameters.filters = filters;

    return function.scanner_producer(function.stream_factory_ptr, parameters);
  }

  static string CompressString(const string &input, const string &location, const flight::FlightDescriptor &descriptor)
  {
    auto codec = arrow::util::Codec::Create(arrow::Compression::ZSTD, 1).ValueOrDie();

    // Estimate the maximum compressed size (usually larger than original size)
    int64_t max_compressed_len = codec->MaxCompressedLen(input.size(), reinterpret_cast<const uint8_t *>(input.data()));

    // Allocate a buffer to hold the compressed data

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(auto compressed_buffer, arrow::AllocateBuffer(max_compressed_len), location, descriptor, "");

    // Perform the compression
    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(auto compressed_size,
                                                       codec->Compress(
                                                           input.size(),
                                                           reinterpret_cast<const uint8_t *>(input.data()),
                                                           max_compressed_len,
                                                           compressed_buffer->mutable_data()),
                                                       location, descriptor, "");

    // If you want to write the compressed data to a string
    std::string compressed_str(reinterpret_cast<const char *>(compressed_buffer->data()), compressed_size);
    return compressed_str;
  }

  static string BuildDynamicTicketData(const string &json_filters, const string &column_ids, uint32_t *uncompressed_length, const string &location, const flight::FlightDescriptor &descriptor)
  {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    // Add key-value pairs to the JSON object
    if (!json_filters.empty())
    {
      yyjson_mut_obj_add_str(doc, root, "airport-duckdb-json-filters", json_filters.c_str());
    }
    if (!column_ids.empty())
    {
      yyjson_mut_obj_add_str(doc, root, "airport-duckdb-column-ids", column_ids.c_str());
    }

    // Serialize the JSON document to a string
    char *metadata_str = yyjson_mut_write(doc, 0, nullptr);

    auto metadata_doc_string = string(metadata_str);

    *uncompressed_length = metadata_doc_string.size();

    auto compressed_metadata = CompressString(metadata_doc_string, location, descriptor);
    free(metadata_str);
    yyjson_mut_doc_free(doc);

    return compressed_metadata;
  }

  unique_ptr<GlobalTableFunctionState> AirportArrowScanInitGlobal(ClientContext &context,
                                                                  TableFunctionInitInput &input)
  {
    auto &bind_data = input.bind_data->Cast<AirportTakeFlightBindData>();

    auto result = make_uniq<ArrowScanGlobalState>();

    arrow::flight::FlightCallOptions call_options;
    airport_add_standard_headers(call_options, bind_data.server_location);

    // Since the ticket is an opaque set of bytes, its useful to the middle ware
    // sometimes to know what the path of the flight is.
    auto descriptor = bind_data.scan_data->flight_descriptor();
    if (descriptor.type == arrow::flight::FlightDescriptor::PATH)
    {
      auto path_parts = descriptor.path;
      std::string joined_path_parts = join_vector_of_strings(path_parts, '/');
      call_options.headers.emplace_back("airport-flight-path", joined_path_parts);
    }

    airport_add_authorization_header(call_options, bind_data.auth_token);

    call_options.headers.emplace_back("airport-trace-id", bind_data.trace_id);

    if (bind_data.skip_producing_result_for_update_or_delete)
    {
      // This is a special case where the result of the scan should be skipped.
      // This is useful when the scan is being used to update or delete rows.
      // For a table that doesn't actually produce row ids, so filtering cannot be applied.
      call_options.headers.emplace_back("airport-skip-producing-results", "1");
    }

    for (const auto &header_pair : bind_data.user_supplied_headers)
    {
      for (const auto &header_value : header_pair.second)
      {
        call_options.headers.emplace_back(header_pair.first, header_value);
      }
    }

    // Rather than using the headers, check to see if the ticket starts with <TICKET_ALLOWS_METADATA>

    // FIXME: right now we're just requesting data from the first endpoint
    // of the flight, in the future we should scan data from all endpoints.
    //
    // we should also pay attention the url scheme of the endpoints because
    // if the server just returns a set of Parquet URLs or HTTP urls we should
    // request those instead.

    auto server_ticket = bind_data.scan_data->flight_info_->endpoints()[0].ticket;
    auto server_ticket_contents = server_ticket.ticket;

    if (!bind_data.ticket.empty())
    {
      server_ticket = flight::Ticket{bind_data.ticket};
    }
    else if (server_ticket_contents.find("<TICKET_ALLOWS_METADATA>") == 0)
    {
      // This ticket allows metadata to be supplied in the ticket.

      auto ticket_without_preamble = server_ticket_contents.substr(strlen("<TICKET_ALLOWS_METADATA>"));

      // encode the length as a unsigned int32 in network byte order
      uint32_t ticket_length = ticket_without_preamble.size();
      auto ticket_length_bytes = std::string((char *)&ticket_length, sizeof(ticket_length));

      uint32_t uncompressed_length;
      // So the column ids can be sent here but there is a special one.
      // COLUMN_IDENTIFIER_ROW_ID that will be sent.
      auto joined_column_ids = join_vector_of_strings(convert_to_strings(input.column_ids), ',');

      auto dynamic_ticket = BuildDynamicTicketData(bind_data.json_filters, joined_column_ids, &uncompressed_length, bind_data.server_location,
                                                   bind_data.scan_data->flight_descriptor());

      auto compressed_length_bytes = std::string((char *)&uncompressed_length, sizeof(uncompressed_length));

      auto manipulated_ticket_data = "<TICKET_WITH_METADATA>" + ticket_length_bytes + ticket_without_preamble + compressed_length_bytes + dynamic_ticket;

      server_ticket = flight::Ticket{manipulated_ticket_data};
    }

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(
        bind_data.scan_data->stream_,
        bind_data.flight_client->DoGet(
            call_options,
            server_ticket),
        bind_data.server_location,
        bind_data.scan_data->flight_descriptor(),
        "");

    //    bind_data.scan_data->stream_ = std::move(flight_stream);
    result->stream = AirportProduceArrowScan(bind_data, input.column_ids, input.filters.get());

    // Since we're single threaded, we can only really use a single thread at a time.
    result->max_threads = 1;

    return std::move(result);
  }

  static double take_flight_scan_progress(ClientContext &, const FunctionData *data, const GlobalTableFunctionState *global_state)
  {
    auto &bind_data = data->Cast<AirportTakeFlightBindData>();
    lock_guard<mutex> guard(bind_data.lock);

    // FIXME: this will have to be adapted for multiple endpoints
    return bind_data.scan_data->progress_ * 100.0;
  }

  void AddTakeFlightFunction(DatabaseInstance &instance)
  {

    auto take_flight_function_set = TableFunctionSet("airport_take_flight");

    auto take_flight_function_with_descriptor = TableFunction(
        "airport_take_flight",
        {LogicalType::VARCHAR, LogicalType::ANY},
        AirportTakeFlight,
        take_flight_bind,
        AirportArrowScanInitGlobal,
        ArrowTableFunction::ArrowScanInitLocal);

    take_flight_function_with_descriptor.named_parameters["auth_token"] = LogicalType::VARCHAR;
    take_flight_function_with_descriptor.named_parameters["secret"] = LogicalType::VARCHAR;
    take_flight_function_with_descriptor.named_parameters["ticket"] = LogicalType::BLOB;
    take_flight_function_with_descriptor.named_parameters["headers"] = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
    take_flight_function_with_descriptor.pushdown_complex_filter = take_flight_complex_filter_pushdown;

    take_flight_function_with_descriptor.cardinality = take_flight_cardinality;
    //    take_flight_function_with_descriptor.get_batch_index = nullptr;
    take_flight_function_with_descriptor.projection_pushdown = true;
    take_flight_function_with_descriptor.filter_pushdown = false;
    take_flight_function_with_descriptor.table_scan_progress = take_flight_scan_progress;
    take_flight_function_set.AddFunction(take_flight_function_with_descriptor);

    auto take_flight_function_with_pointer = TableFunction(
        "airport_take_flight",
        {LogicalType::VARCHAR, LogicalType::POINTER},
        AirportTakeFlight,
        take_flight_bind_with_pointer,
        AirportArrowScanInitGlobal,
        ArrowTableFunction::ArrowScanInitLocal);

    take_flight_function_with_pointer.named_parameters["auth_token"] = LogicalType::VARCHAR;
    take_flight_function_with_pointer.named_parameters["secret"] = LogicalType::VARCHAR;
    take_flight_function_with_pointer.pushdown_complex_filter = take_flight_complex_filter_pushdown;

    // Add support for optional named paraemters that would be appended to the descriptor
    // of the flight, ideally parameters would be JSON encoded.

    take_flight_function_with_pointer.cardinality = take_flight_cardinality;
    //    take_flight_function_with_pointer.get_batch_index = nullptr;
    take_flight_function_with_pointer.projection_pushdown = true;
    take_flight_function_with_pointer.filter_pushdown = false;
    take_flight_function_with_pointer.table_scan_progress = take_flight_scan_progress;
    take_flight_function_with_pointer.statistics = take_flight_statistics;

    take_flight_function_set.AddFunction(take_flight_function_with_pointer);

    ExtensionUtil::RegisterFunction(instance, take_flight_function_set);
  }
}