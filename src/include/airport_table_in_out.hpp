#pragma once

#include "duckdb/planner/planner_extension.hpp"

namespace duckdb
{
  class TableFunction;
  bool IsAirportTableInOutFunction(const TableFunction &function);

  // Table-input Flight exchanges must finish once per query, not once per pipeline.
  void AirportPlanTableInOut(PlannerExtensionInput &input, BoundStatement &statement);
}
