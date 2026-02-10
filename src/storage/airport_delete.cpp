#include "duckdb.hpp"
#include "storage/airport_delete.hpp"
#include "storage/airport_table_entry.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "storage/airport_catalog.hpp"
#include "storage/airport_transaction.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "airport_macros.hpp"
#include "airport_request_headers.hpp"
#include "airport_flight_exception.hpp"
#include "airport_secrets.hpp"
#include "storage/airport_delete_parameterized.hpp"

#include "duckdb/common/arrow/schema_metadata.hpp"

#include "airport_flight_stream.hpp"
#include "airport_take_flight.hpp"
#include "storage/airport_exchange.hpp"
#include "airport_logging.hpp"

// Some improvements to make
//
// The global state needs to accumulate data chunks that are returned by the local
// returned delete calls. This is because the data is returned in chunks and we need
// to process it.
//
// It seems that upon delete all columns of the table are returned, but it seems reasonable.
//
// We need to keep a local state and a global state.
//
// Reference physical_delete.cpp for ideas around the implementation.
//
// Need to add the code to read the returned chunks for the DoExchange call, which means we'll
// be dealing with ArrowScan again, but hopefully in a more limited way since we're just
// dealing with DataChunks, but it could be more since we aren't just faking a function call.
//
// Transactional Guarantees:
//
// There really won't be many guarantees - since all row ids can't be pushed in one call
// it could really be up to the server to determine if the operation succeeded.
//
// DoExchange could just be used for a chunked delete, and then finally a commit or rollback
// action is sent at the end of the calls. But this could be really hard on the remote server to
// implement, since it would have to deal with transactional problems.
//
// Is there some way to keep at the flight stream to determine if there is data to read on the stream?
// If so it could be a single DoExchange call.
//
// Could be simulated with flow control with metadata messages, but need to cast a metadata reader
// rather than just a stream reader.
//
//

namespace duckdb
{

  AirportDelete::AirportDelete(PhysicalPlan &physical_plan,
                               vector<LogicalType> types,
                               TableCatalogEntry &table,
                               vector<unique_ptr<BoundConstraint>> bound_constraints,
                               const idx_t rowid_index,
                               idx_t estimated_cardinality,
                               const bool return_chunk)
      : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality), table(table),
        bound_constraints(std::move(bound_constraints)), rowid_index(rowid_index), return_chunk(return_chunk)
  {
  }

  class AirportDeleteLocalState : public LocalSinkState
  {
  public:
    AirportDeleteLocalState(ClientContext &context, TableCatalogEntry &table)
    //                            const vector<unique_ptr<BoundConstraint>> &bound_constraints)
    {
      delete_chunk.Initialize(Allocator::Get(context), table.GetTypes());
    }
    DataChunk delete_chunk;
  };

  class AirportDeleteGlobalState : public GlobalSinkState, public AirportExchangeGlobalState
  {
  public:
    explicit AirportDeleteGlobalState(
        ClientContext &context,
        AirportTableEntry &table,
        const vector<LogicalType> &return_types,
        bool return_chunk) : changed_count(0), return_chunk(return_chunk), table(table),
                             return_collection(context, return_types)
    {
    }

    mutex delete_lock;
    idx_t changed_count;

    // Is there any data requested to be returned.
    bool return_chunk;

    AirportTableEntry &table;
    ColumnDataCollection return_collection;

    void Flush(ClientContext &context)
    {
    }
  };

  unique_ptr<GlobalSinkState> AirportDelete::GetGlobalSinkState(ClientContext &context) const
  {
    auto &airport_table = table.Cast<AirportTableEntry>();

    auto delete_global_state = make_uniq<AirportDeleteGlobalState>(context, airport_table, GetTypes(), return_chunk);

    auto &transaction = AirportTransaction::Get(context, table.catalog);

    delete_global_state->send_types = {airport_table.GetRowIdType()};
    vector<string> send_names = {"rowid"};
    ArrowSchema send_schema;
    auto client_properties = context.GetClientProperties();
    ArrowConverter::ToArrowSchema(&send_schema, delete_global_state->send_types, send_names,
                                  client_properties);

    vector<string> returning_column_names;
    returning_column_names.reserve(table.GetColumns().LogicalColumnCount());
    for (auto &cd : table.GetColumns().Logical())
    {
      returning_column_names.push_back(cd.GetName());
    }

    AirportExchangeGetGlobalSinkState(context, table, airport_table, delete_global_state.get(), send_schema, return_chunk, "delete",
                                      returning_column_names,
                                      transaction.identifier());

    return delete_global_state;
  }

  unique_ptr<LocalSinkState> AirportDelete::GetLocalSinkState(ExecutionContext &context) const
  {
    return make_uniq<AirportDeleteLocalState>(context.client, table);
  }

  //===--------------------------------------------------------------------===//
  // Sink
  //===--------------------------------------------------------------------===//
  SinkResultType AirportDelete::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const
  {
    auto &gstate = input.global_state.Cast<AirportDeleteGlobalState>();
    auto &ustate = input.local_state.Cast<AirportDeleteLocalState>();

    // Since we need to return the data from the rows that we're deleting.
    // we need do exchanges with the server chunk by chunk because if we batch everything
    // up it could use a lot of memory and we wouldn't be able to return the data
    // to the user.

    // Somehow we're getting a chunk with 2 columns,
    // but we're only expecting one column.

    // So it turns out the chunk that it passed may have additional colums included,
    // especially if filtering is being applied, but we need to only send the row id column.
    auto small_chunk = DataChunk();
    small_chunk.Initialize(context.client, gstate.send_types, chunk.size());
    small_chunk.data[0].Reference(chunk.data[rowid_index]);
    small_chunk.SetCardinality(chunk.size());

    auto appender = make_uniq<ArrowAppender>(gstate.send_types, small_chunk.size(), context.client.GetClientProperties(),
                                             ArrowTypeExtensionData::GetExtensionTypes(
                                                 context.client, gstate.send_types));
    appender->Append(small_chunk, 0, small_chunk.size(), small_chunk.size());
    ArrowArray arr = appender->Finalize();

    AIRPORT_ASSIGN_OR_RAISE_CONTAINER(
        auto record_batch,
        arrow::ImportRecordBatch(&arr, gstate.send_schema),
        gstate.table.table_data,
        "");

    // Acquire a lock because we don't want other threads to be writing to the same streams
    // at the same time.
    lock_guard<mutex> delete_guard(gstate.delete_lock);

    AIRPORT_ARROW_ASSERT_OK_CONTAINER(
        gstate.writer->WriteRecordBatch(*record_batch),
        gstate.table.table_data, "");

    // Since we wrote a batch I'd like to read the data returned if we are returning chunks.
    if (gstate.return_chunk)
    {
      gstate.ReadDataIntoChunk(ustate.delete_chunk);
      gstate.return_collection.Append(ustate.delete_chunk);
    }
    return SinkResultType::NEED_MORE_INPUT;
  }

  //===--------------------------------------------------------------------===//
  // Finalize
  //===--------------------------------------------------------------------===//
  SinkFinalizeType AirportDelete::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                           OperatorSinkFinalizeInput &input) const
  {
    auto &gstate = input.global_state.Cast<AirportDeleteGlobalState>();

    // printf("AirportDelete::Finalize started, indicating that writing is done\n");

    AIRPORT_ARROW_ASSERT_OK_CONTAINER(
        gstate.writer->DoneWriting(),
        gstate.table.table_data, "");

    auto stats = gstate.writer->stats();
    DUCKDB_LOG(context, AirportLogType, "Delete Write Stats", {{"num_messages", to_string(stats.num_messages)}, {"num_record_batches", to_string(stats.num_record_batches)}, {"num_dictionary_batches", to_string(stats.num_dictionary_batches)}, {"num_dictionary_deltas", to_string(stats.num_dictionary_deltas)}, {"num_replaced_dictionaries", to_string(stats.num_replaced_dictionaries)}, {"total_raw_body_size", to_string(stats.total_raw_body_size)}, {"total_serialized_body_size", to_string(stats.total_serialized_body_size)}});

    try
    {
      auto changed_count = gstate.ReadChangedCount(gstate.table.table_data->server_location());
      if (changed_count)
      {
        gstate.changed_count = *changed_count;
      }
    }
    catch (...)
    {
      auto result = gstate.writer->Close();
      throw;
    }

    return SinkFinalizeType::READY;
  }

  //===--------------------------------------------------------------------===//
  // Source
  //===--------------------------------------------------------------------===//
  class AirportDeleteSourceState : public GlobalSourceState
  {
  public:
    explicit AirportDeleteSourceState(const AirportDelete &op)
    {
      if (op.return_chunk)
      {
        D_ASSERT(op.sink_state);
        auto &g = op.sink_state->Cast<AirportDeleteGlobalState>();
        g.return_collection.InitializeScan(scan_state);
      }
    }

    ColumnDataScanState scan_state;
  };

  unique_ptr<GlobalSourceState> AirportDelete::GetGlobalSourceState(ClientContext &context) const
  {
    return make_uniq<AirportDeleteSourceState>(*this);
  }

  //===--------------------------------------------------------------------===//
  // GetData
  //===--------------------------------------------------------------------===//
  SourceResultType AirportDelete::GetData(ExecutionContext &context, DataChunk &chunk,
                                          OperatorSourceInput &input) const
  {
    auto &state = input.global_state.Cast<AirportDeleteSourceState>();
    auto &g = sink_state->Cast<AirportDeleteGlobalState>();
    if (!return_chunk)
    {
      chunk.SetCardinality(1);
      chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(g.changed_count)));
      return SourceResultType::FINISHED;
    }

    g.return_collection.Scan(state.scan_state, chunk);

    return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
  }

  //===--------------------------------------------------------------------===//
  // Helpers
  //===--------------------------------------------------------------------===//
  string AirportDelete::GetName() const
  {
    return "AIRPORT_DELETE";
  }

  InsertionOrderPreservingMap<string> AirportDelete::ParamsToString() const
  {
    InsertionOrderPreservingMap<string> result;
    result["Table Name"] = table.name;
    return result;
  }

  //===--------------------------------------------------------------------===//
  // Plan
  //===--------------------------------------------------------------------===//
  PhysicalOperator &AirportCatalog::PlanDelete(ClientContext &context,
                                               PhysicalPlanGenerator &planner,
                                               LogicalDelete &op,
                                               PhysicalOperator &plan)
  {
    auto &bound_ref = op.expressions[0]->Cast<BoundReferenceExpression>();
    // AirportCatalog::MaterializeAirportScans(*plan);
    auto &airport_table = op.table.Cast<AirportTableEntry>();

    if (airport_table.GetRowIdType() == LogicalType::SQLNULL)
    {
      if (op.return_chunk)
      {
        throw BinderException("RETURNING clause not yet supported for parameterized delete using an Airport table");
      }

      auto &del = planner.Make<AirportDeleteParameterized>(op, op.table, plan);
      del.children.push_back(plan);
      return del;
    }

    auto &del = planner.Make<AirportDelete>(op.types, op.table, std::move(op.bound_constraints), bound_ref.index, op.estimated_cardinality, op.return_chunk);
    del.children.push_back(plan);
    return del;
  }

} // namespace duckdb
