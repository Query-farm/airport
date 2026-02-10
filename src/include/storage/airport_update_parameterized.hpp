#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb
{
  class AirportUpdateParameterized : public PhysicalOperator
  {
  public:
    AirportUpdateParameterized(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table, PhysicalOperator &plan);

    //! The table to delete from
    TableCatalogEntry &table;
    string sql_filters;

  public:
    bool IsSource() const override
    {
      return true;
    }

    // Source interface
    SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

  public:
    // Sink interface
    unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
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
