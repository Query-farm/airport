#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/index_vector.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"

namespace duckdb
{

  class AirportInsertGlobalState;
  class AirportInsertLocalState;
  class AirportInsert : public PhysicalOperator
  {
  public:
    //! INSERT INTO
    AirportInsert(PhysicalPlan &physical_plan,
                  vector<LogicalType> types,
                  TableCatalogEntry &table,
                  vector<unique_ptr<BoundConstraint>> bound_constraints,
                  vector<unique_ptr<Expression>> set_expressions,
                  vector<PhysicalIndex> set_columns,
                  vector<LogicalType> set_types,
                  physical_index_vector_t<idx_t> column_index_map,
                  idx_t estimated_cardinality,
                  bool return_chunk,
                  OnConflictAction action_type,
                  unique_ptr<Expression> on_conflict_condition,
                  unique_ptr<Expression> do_update_condition,
                  unordered_set<column_t> on_conflict_filter,
                  vector<column_t> columns_to_fetch,
                  vector<unique_ptr<Expression>> bound_defaults);

    //! CREATE TABLE AS
    AirportInsert(PhysicalPlan &physical_plan,
                  LogicalOperator &op, SchemaCatalogEntry &schema, unique_ptr<BoundCreateTableInfo> info, idx_t estimated_cardinality);

  private:
    //! The table to insert into
    optional_ptr<TableCatalogEntry> insert_table;
    // optional_ptr<TableCatalogEntry> table;

    //! The insert types
    vector<LogicalType> insert_types;

    //! Table schema, in case of CREATE TABLE AS
    optional_ptr<SchemaCatalogEntry> schema;
    //! Create table info, in case of CREATE TABLE AS
    unique_ptr<BoundCreateTableInfo> info;

  public:
    //! column_index_map
    physical_index_vector_t<idx_t> column_index_map;

  public:
    const bool return_chunk;

  private:
    //! The default expressions of the columns for which no value is provided
    const vector<unique_ptr<Expression>> bound_defaults;
    //! The bound constraints for the table
    const vector<unique_ptr<BoundConstraint>> bound_constraints;

    // The DO UPDATE set expressions, if 'action_type' is UPDATE
    vector<unique_ptr<Expression>> set_expressions;
    // Which columns are targeted by the set expressions
    vector<PhysicalIndex> set_columns;

    // For now always just throw errors.
    OnConflictAction action_type = OnConflictAction::THROW;

    // The types of the columns targeted by a SET expression
    vector<LogicalType> set_types;

    // Condition for the ON CONFLICT clause
    unique_ptr<Expression> on_conflict_condition;
    // Condition for the DO UPDATE clause
    unique_ptr<Expression> do_update_condition;
    // The column ids to apply the ON CONFLICT on
    unordered_set<column_t> conflict_target;

  public:
    // Source interface
    SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

    bool IsSource() const override
    {
      return true;
    }

  protected:
    idx_t OnConflictHandling(TableCatalogEntry &table,
                             ExecutionContext &context,
                             AirportInsertGlobalState &gstate,
                             AirportInsertLocalState &lstate,
                             DataChunk &chunk) const;

  private:
    static void ResolveDefaults(const TableCatalogEntry &table, DataChunk &chunk,
                                const physical_index_vector_t<idx_t> &column_index_map,
                                ExpressionExecutor &default_executor, DataChunk &result);

  public:
    // Sink interface
    unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
    unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;

    unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;

    SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
    SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                              OperatorSinkFinalizeInput &input) const override;

    bool IsSink() const override
    {
      return true;
    }

    bool ParallelSink() const override
    {
      return false;
    }

    string GetName() const override;
    InsertionOrderPreservingMap<string> ParamsToString() const override;
  };

}
