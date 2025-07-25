#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb
{
  class AirportDelete : public PhysicalOperator
  {
  public:
    AirportDelete(PhysicalPlan &physical_plan,
                  vector<LogicalType> types,
                  TableCatalogEntry &table,
                  vector<unique_ptr<BoundConstraint>> bound_constraints,
                  const idx_t rowid_index,
                  idx_t estimated_cardinality,
                  const bool return_chunk);

  private:
    //! The table to delete from
    TableCatalogEntry &table;
    vector<unique_ptr<BoundConstraint>> bound_constraints;

  public:
    const idx_t rowid_index;
    const bool return_chunk;

  public:
    // Source interface
    SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

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
  };

} // namespace duckdb
