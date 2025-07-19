#include "storage/airport_update.hpp"
#include "storage/airport_table_entry.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "storage/airport_catalog.hpp"
#include "storage/airport_transaction.hpp"
#include "duckdb/common/types/uuid.hpp"

#include "duckdb/common/arrow/schema_metadata.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"

#include "airport_flight_stream.hpp"
#include "airport_take_flight.hpp"
#include "storage/airport_exchange.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include "airport_macros.hpp"
#include "storage/airport_update_parameterized.hpp"
#include "airport_request_headers.hpp"
#include "airport_flight_exception.hpp"
#include "airport_secrets.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb
{

  AirportUpdate::AirportUpdate(
      PhysicalPlan &physical_plan,
      LogicalOperator &op,
      vector<LogicalType> types,
      TableCatalogEntry &table,
      vector<PhysicalIndex> columns, vector<unique_ptr<Expression>> expressions,
      vector<unique_ptr<Expression>> bound_defaults, vector<unique_ptr<BoundConstraint>> bound_constraints,
      idx_t estimated_cardinality, bool return_chunk, bool update_is_del_and_insert)
      : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, estimated_cardinality),
        table(table),
        columns(std::move(columns)), expressions(std::move(expressions)),
        bound_defaults(std::move(bound_defaults)), bound_constraints(std::move(bound_constraints)),
        update_is_del_and_insert(update_is_del_and_insert),
        return_chunk(return_chunk)
  {
    auto &table_columns = table.GetColumns();
    auto &airport_table = table.Cast<AirportTableEntry>();

    // Save the names of the column expression that will be sent to
    // the remote server.
    D_ASSERT(this->columns.size() > 0);
    D_ASSERT(this->expressions.size() > 0);
    for (auto &column_index : this->columns)
    {
      send_names.push_back(table_columns.GetColumn(column_index).GetName());
    }
    // This is always sent last.
    send_names.push_back("rowid");

    for (auto &expr : this->expressions)
    {
      send_types.push_back(expr->return_type);
    }
    send_types.emplace_back(airport_table.GetRowIdType());

    D_ASSERT(send_names.size() == send_types.size());

    // for (size_t i = 0; i < send_types.size(); i++)
    // {
    //   printf("Sending index %ld name=%s type=%s\n", i, send_names[i].c_str(), send_types[i].ToString().c_str());
    // }
  }

  //===--------------------------------------------------------------------===//
  // States
  //===--------------------------------------------------------------------===//
  class AirportUpdateGlobalState : public GlobalSinkState, public AirportExchangeGlobalState
  {
  public:
    explicit AirportUpdateGlobalState(
        ClientContext &context,
        AirportTableEntry &table,
        const vector<LogicalType> &return_types,
        bool return_chunk) : table(table), changed_count(0),
                             return_collection(context, return_types), return_chunk(return_chunk)

    {
      // printf("Initalizing with return types %ld\n", return_types.size());
      // for (auto &type : return_types)
      // {
      //   printf("Type: %s\n", type.ToString().c_str());
      // }
      // printf("Finished return types\n");
    }
    AirportTableEntry &table;

    mutex update_lock;
    idx_t changed_count;

    ColumnDataCollection return_collection;

    const bool return_chunk;
  };

  class AirportUpdateLocalState : public LocalSinkState
  {
  public:
    AirportUpdateLocalState(ClientContext &context,
                            const TableCatalogEntry &table,
                            const vector<unique_ptr<Expression>> &expressions,
                            const vector<LogicalType> &table_types,
                            const vector<unique_ptr<Expression>> &bound_defaults,
                            const vector<unique_ptr<BoundConstraint>> &bound_constraints,
                            const vector<LogicalType> &update_types)
        : default_executor(context, bound_defaults),
          bound_constraints(bound_constraints)
    {
      auto &allocator = Allocator::Get(context);
      D_ASSERT(update_types.size() == expressions.size() + 1);
      read_from_flight_chunk.Initialize(allocator, update_types);
      table_mock_chunk.Initialize(allocator, table_types);
    }

    // This is the DataChunk that has the type of the update that will
    // be returned from the external service.
    DataChunk read_from_flight_chunk;

    // This is the DataChunk that has the types of the table and in
    // the order of the columns of the table
    DataChunk table_mock_chunk;

    ExpressionExecutor default_executor;
    const vector<unique_ptr<BoundConstraint>> &bound_constraints;
  };

  unique_ptr<GlobalSinkState> AirportUpdate::GetGlobalSinkState(ClientContext &context) const
  {
    auto &airport_table = this->table.Cast<AirportTableEntry>();

    auto update_global_state = make_uniq<AirportUpdateGlobalState>(
        context,
        airport_table,
        GetTypes(),
        return_chunk);

    auto &transaction = AirportTransaction::Get(context, table.catalog);

    update_global_state->send_types = send_types;

    ArrowSchema send_schema;
    auto client_properties = context.GetClientProperties();

    ArrowConverter::ToArrowSchema(&send_schema,
                                  update_global_state->send_types,
                                  send_names,
                                  client_properties);

    // Get the names of the columns that exist on the table with the addition of
    // rowid which will always be the last column.
    vector<string> table_column_names;
    for (auto &cd : this->table.GetColumns().Physical())
    {
      table_column_names.push_back(cd.GetName());
    }
    table_column_names.push_back("rowid");

    AirportExchangeGetGlobalSinkState(context,
                                      table,
                                      airport_table,
                                      update_global_state.get(),
                                      send_schema,
                                      return_chunk,
                                      "update",
                                      table_column_names,
                                      transaction.identifier());

    return update_global_state;
  }

  unique_ptr<LocalSinkState> AirportUpdate::GetLocalSinkState(ExecutionContext &context) const
  {
    return make_uniq<AirportUpdateLocalState>(context.client,
                                              table,
                                              expressions,
                                              table.GetTypes(),
                                              bound_defaults,
                                              bound_constraints,
                                              send_types);
  }

  //===--------------------------------------------------------------------===//
  // Sink
  //===--------------------------------------------------------------------===//
  SinkResultType AirportUpdate::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const
  {
    auto &gstate = input.global_state.Cast<AirportUpdateGlobalState>();
    auto &lstate = input.local_state.Cast<AirportUpdateLocalState>();

    DataChunk &send_update_chunk = lstate.read_from_flight_chunk;

    chunk.Flatten();
    lstate.default_executor.SetChunk(chunk);

    // printf("Sending chunk column count: %llu\n", chunk.ColumnCount());
    // printf("Chunk is %s\n", chunk.ToString().c_str());

    send_update_chunk.Reset();
    send_update_chunk.SetCardinality(chunk);

    // Evaluate all of the necessary expressions.
    for (idx_t i = 0; i < expressions.size(); i++)
    {
      if (expressions[i]->type == ExpressionType::VALUE_DEFAULT)
      {
        // default expression, set to the default value of the column
        lstate.default_executor.ExecuteExpression(columns[i].index, send_update_chunk.data[i]);
      }
      else
      {
        D_ASSERT(expressions[i]->type == ExpressionType::BOUND_REF);
        // index into child chunk
        auto &binding = expressions[i]->Cast<BoundReferenceExpression>();
        send_update_chunk.data[i].Reference(chunk.data[binding.index]);
      }
    }

    send_update_chunk.data[expressions.size()].Reference(chunk.data[chunk.ColumnCount() - 1]);

    lock_guard<mutex> update_guard(gstate.update_lock);
    // Acquire a lock because we don't want other threads to be writing to the same streams
    // at the same time.

    auto appender = make_uniq<ArrowAppender>(gstate.send_types, send_update_chunk.size(), context.client.GetClientProperties(),
                                             ArrowTypeExtensionData::GetExtensionTypes(
                                                 context.client, gstate.send_types));
    appender->Append(send_update_chunk, 0, send_update_chunk.size(), send_update_chunk.size());
    ArrowArray arr = appender->Finalize();

    // Import the record batch into the the C++ side of Arrow and write it
    // to the stream.

    AIRPORT_ASSIGN_OR_RAISE_CONTAINER(
        auto record_batch,
        arrow::ImportRecordBatch(&arr, gstate.send_schema),
        gstate.table.table_data, "");

    AIRPORT_ARROW_ASSERT_OK_CONTAINER(
        gstate.writer->WriteRecordBatch(*record_batch),
        gstate.table.table_data, "");

    // Since we wrote a batch I'd like to read the data returned if we are returning chunks.
    if (gstate.return_chunk)
    {
      AirportExchangeReadDataToChunk(
          gstate.scan_table_function_input->bind_data->Cast<AirportTakeFlightBindData>(),
          gstate.scan_table_function_input->local_state->Cast<AirportArrowScanLocalState>(),
          lstate.read_from_flight_chunk);

      DataChunk &mock_chunk = lstate.table_mock_chunk;

      mock_chunk.SetCardinality(lstate.read_from_flight_chunk.size());
      for (idx_t i = 0; i < columns.size(); i++)
      {
        mock_chunk.data[columns[i].index].Reference(lstate.read_from_flight_chunk.data[i]);
      }

      lstate.table_mock_chunk.Verify();

      // printf("Read chunk to return:%s\n", mock_chunk.ToString().c_str());

      // Now the problem is the rowid column is being returned from the remote server
      gstate.return_collection.Append(mock_chunk);
    }

    return SinkResultType::NEED_MORE_INPUT;
  }

  struct AirportUpdateFinalMetadata
  {
    uint64_t total_updated;
    MSGPACK_DEFINE_MAP(total_updated)
  };

  //===--------------------------------------------------------------------===//
  // Finalize
  //===--------------------------------------------------------------------===//
  SinkFinalizeType AirportUpdate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                           OperatorSinkFinalizeInput &input) const
  {
    auto &gstate = input.global_state.Cast<AirportUpdateGlobalState>();

    AIRPORT_ARROW_ASSERT_OK_CONTAINER(
        gstate.writer->DoneWriting(),
        gstate.table.table_data, "");

    auto changed_count = gstate.ReadChangedCount(gstate.table.table_data->server_location());
    if (changed_count)
    {
      gstate.changed_count = *changed_count;
    }

    return SinkFinalizeType::READY;
  }

  class AirportUpdateSourceState : public GlobalSourceState
  {
  public:
    explicit AirportUpdateSourceState(const AirportUpdate &op)
    {
      if (op.return_chunk)
      {
        D_ASSERT(op.sink_state);
        auto &g = op.sink_state->Cast<AirportUpdateGlobalState>();
        g.return_collection.InitializeScan(scan_state);
      }
    }

    ColumnDataScanState scan_state;
  };

  unique_ptr<GlobalSourceState> AirportUpdate::GetGlobalSourceState(ClientContext &context) const
  {
    return make_uniq<AirportUpdateSourceState>(*this);
  }

  //===--------------------------------------------------------------------===//
  // GetData
  //===--------------------------------------------------------------------===//
  SourceResultType AirportUpdate::GetData(ExecutionContext &context, DataChunk &chunk,
                                          OperatorSourceInput &input) const
  {
    auto &state = input.global_state.Cast<AirportUpdateSourceState>();
    auto &g = sink_state->Cast<AirportUpdateGlobalState>();
    if (!return_chunk)
    {
      chunk.SetCardinality(1);
      chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(g.changed_count)));
      return SourceResultType::FINISHED;
    }

    // So it turns out the final chunk is returned in the same order
    // as the column in the table.
    //
    // FIX THIS in the monring.

    g.return_collection.Scan(state.scan_state, chunk);

    return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
  }

  //===--------------------------------------------------------------------===//
  // Helpers
  //===--------------------------------------------------------------------===//
  string AirportUpdate::GetName() const
  {
    return "AIRPORT_UPDATE";
  }

  InsertionOrderPreservingMap<string> AirportUpdate::ParamsToString() const
  {
    InsertionOrderPreservingMap<string> result;
    result["Table Name"] = table.name;
    return result;
  }

  //===--------------------------------------------------------------------===//
  // Plan
  //===--------------------------------------------------------------------===//
  PhysicalOperator &AirportCatalog::PlanUpdate(ClientContext &context,
                                               PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                               PhysicalOperator &plan)
  {
    auto &airport_table = op.table.Cast<AirportTableEntry>();
    for (const auto &expr : op.expressions)
    {
      if (expr->type == ExpressionType::VALUE_DEFAULT)
      {
        throw BinderException("SET DEFAULT is not yet supported for updates of a Airport table");
      }
    }

    if (airport_table.GetRowIdType() == LogicalType::SQLNULL)
    {
      if (op.return_chunk)
      {
        throw BinderException("RETURNING clause not yet supported for parameterized update of an Airport table");
      }

      auto &upd = planner.Make<AirportUpdateParameterized>(op, op.table, plan);
      upd.children.push_back(plan);
      return upd;
    }

    auto &update = planner.Make<AirportUpdate>(op,
                                               op.types,
                                               op.table,
                                               op.columns,
                                               std::move(op.expressions), std::move(op.bound_defaults),
                                               std::move(op.bound_constraints),
                                               op.estimated_cardinality,
                                               op.return_chunk,
                                               op.update_is_del_and_insert);

    update.children.push_back(plan);
    return update;
  }

} // namespace duckdb
