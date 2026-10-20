#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "airport_flight_stream.hpp"
#include "airport_take_flight.hpp"
#include "airport_table_entry.hpp"
#include "airport_schema_utils.hpp"

namespace duckdb
{

  namespace
  {
    struct AirportChangedFinalMetadata
    {
      uint64_t total_changed;
      MSGPACK_DEFINE_MAP(total_changed)
    };
  }

  struct AirportExchangeTakeFlightBindData : public AirportTakeFlightBindData
  {

  public:
    explicit AirportExchangeTakeFlightBindData(
        stream_factory_produce_t scanner_producer_p,
        const string &trace_id,
        const int64_t estimated_records,
        const AirportTakeFlightParameters &take_flight_params_p,
        const std::optional<AirportTableFunctionFlightInfoParameters> &table_function_parameters_p,
        std::shared_ptr<arrow::Schema> schema,
        const flight::FlightDescriptor &descriptor, AirportTableEntry *table_entry,
        shared_ptr<DependencyItem> dependency = nullptr)
        : AirportTakeFlightBindData(
              scanner_producer_p,
              trace_id,
              estimated_records,
              take_flight_params_p,
              table_function_parameters_p,
              schema,
              descriptor,
              table_entry,
              std::move(dependency))
    {
    }

    mutable mutex lock;

    void examine_schema(
        ClientContext &context,
        bool skip_rowid_column)
    {
      AirportExamineSchema(
          context,
          schema_root,
          &arrow_table,
          &return_types_,
          &names_,
          nullptr,
          &rowid_column_index,
          skip_rowid_column);
    }

    const vector<string> &names() const
    {
      return names_;
    }

    const vector<LogicalType> &return_types() const
    {
      return return_types_;
    }

  private:
    // these are set in examine_schema().
    vector<string> names_;
    vector<LogicalType> return_types_;
  };

  // This is all of the state is needed to perform a ArrowScan on a resulting
  // DoExchange stream, this is useful for having RETURNING data work for
  // INSERT, DELETE or UPDATE.
  class AirportExchangeGlobalState
  {
  public:
    std::shared_ptr<arrow::Schema> send_schema;

    std::unique_ptr<AirportExchangeTakeFlightBindData> scan_bind_data;
    std::unique_ptr<ArrowArrayStreamWrapper> reader;
    std::unique_ptr<arrow::flight::FlightStreamWriter> writer;

    duckdb::unique_ptr<TableFunctionInput> scan_table_function_input;

    duckdb::unique_ptr<GlobalTableFunctionState> scan_global_state;
    duckdb::unique_ptr<LocalTableFunctionState> local_state;

    vector<LogicalType> send_types;
    vector<string> send_names;

    void ReadDataIntoChunk(DataChunk &dest)
    {
      auto &data = scan_table_function_input->bind_data->Cast<AirportTakeFlightBindData>();
      auto &state = scan_table_function_input->local_state->Cast<AirportArrowScanLocalState>();

      dest.Reset();
      state.Reset();
      state.chunk = state.stream()->GetNextChunk();

      auto output_size =
          MinValue<idx_t>(STANDARD_VECTOR_SIZE, NumericCast<idx_t>(state.chunk->arrow_array.length) - state.chunk_offset);
      dest.SetCardinality(output_size);

      // We should only get the number of rows == to the number of rows sent in the chunk
      // to the server.
      D_ASSERT(output_size <= STANDARD_VECTOR_SIZE);

      ArrowTableFunction::ArrowToDuckDB(state,
                                        data.arrow_table.GetColumns(),
                                        dest,
                                        false);
      dest.Verify();
    }

    //! The DuckDB type of the rowid column the server returned, or SQLNULL if the
    //! server's returned schema has no column marked as the rowid.
    LogicalType ReturnedRowIdType() const
    {
      auto &data = scan_table_function_input->bind_data->Cast<AirportTakeFlightBindData>();
      if (data.rowid_column_index == COLUMN_IDENTIFIER_ROW_ID)
      {
        return LogicalType::SQLNULL;
      }
      return data.arrow_table.GetColumns().at(COLUMN_IDENTIFIER_ROW_ID)->GetDuckType();
    }

    //! Read the server's rowid column from the batch most recently read by
    //! ReadDataIntoChunk() into the single column of dest, which must have the
    //! type returned by ReturnedRowIdType().
    void ReadRowIdIntoChunk(DataChunk &dest, idx_t count)
    {
      auto &data = scan_table_function_input->bind_data->Cast<AirportTakeFlightBindData>();
      auto &state = scan_table_function_input->local_state->Cast<AirportArrowScanLocalState>();
      D_ASSERT(data.rowid_column_index != COLUMN_IDENTIFIER_ROW_ID);
      D_ASSERT(dest.ColumnCount() == 1);

      dest.Reset();
      dest.SetCardinality(count);

      // Temporarily project only the rowid column, the scan maps it to the
      // Arrow column marked as the rowid.
      auto saved_column_ids = std::move(state.column_ids);
      state.column_ids = {COLUMN_IDENTIFIER_ROW_ID};
      try
      {
        ArrowTableFunction::ArrowToDuckDB(state,
                                          data.arrow_table.GetColumns(),
                                          dest,
                                          false,
                                          data.rowid_column_index);
      }
      catch (...)
      {
        state.column_ids = std::move(saved_column_ids);
        throw;
      }
      state.column_ids = std::move(saved_column_ids);
      dest.Verify();
    }

    std::optional<uint64_t> ReadChangedCount(const string &server_location)
    {
      auto &bind_data = scan_table_function_input->bind_data->Cast<AirportTakeFlightBindData>();
      auto &local_state = scan_table_function_input->local_state->Cast<AirportArrowScanLocalState>();

      local_state.Reset();
      local_state.chunk = local_state.stream()->GetNextChunk();

      auto &last_app_metadata = bind_data.last_app_metadata;
      if (last_app_metadata)
      {
        AirportChangedFinalMetadata final_metadata;
        AIRPORT_MSGPACK_UNPACK(final_metadata,
                               (*last_app_metadata),
                               server_location,
                               "Failed to parse msgpack encoded object for final update metadata.");
        return final_metadata.total_changed;
      }
      return std::nullopt;
    }
  };

  void AirportExchangeGetGlobalSinkState(ClientContext &context,
                                         const TableCatalogEntry &table,
                                         const AirportTableEntry &airport_table,
                                         AirportExchangeGlobalState *global_state,
                                         const ArrowSchema &send_schema,
                                         const bool return_chunk,
                                         const string exchange_operation,
                                         const vector<string> returning_column_names,
                                         const std::optional<string> transaction_id);

}