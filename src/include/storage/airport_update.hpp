#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/index_vector.hpp"

namespace duckdb
{

  class AirportUpdate : public PhysicalOperator
  {
  public:
    AirportUpdate(
        PhysicalPlan &physical_plan,
        vector<LogicalType> types,
        TableCatalogEntry &table,
        vector<PhysicalIndex> columns, vector<unique_ptr<Expression>> expressions,
        vector<unique_ptr<Expression>> bound_defaults, vector<unique_ptr<BoundConstraint>> bound_constraints,
        idx_t estimated_cardinality, bool return_chunk,
        bool update_is_del_and_insert);

  private:
    //! The table to delete from
    TableCatalogEntry &table;
    //! The set of columns to update
    vector<PhysicalIndex> columns;

    vector<unique_ptr<Expression>> expressions;
    vector<unique_ptr<Expression>> bound_defaults;
    vector<unique_ptr<BoundConstraint>> bound_constraints;

  public:
    const bool update_is_del_and_insert;
    //! If the returning statement is present, return the whole chunk
    const bool return_chunk;

  public:
    // Source interface
    SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

    bool IsSource() const override
    {
      return true;
    }

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

  private:
    vector<LogicalType> send_types;
    // This is a list of all column names that will be send
    vector<string> send_names;
  };
}
