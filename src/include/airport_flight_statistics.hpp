#pragma once
#include "airport_extension.hpp"
#include "duckdb.hpp"

namespace duckdb
{
  unique_ptr<BaseStatistics> AirportTakeFlightStatistics(ClientContext &context, const FunctionData *bind_data, const column_t column_index);

}