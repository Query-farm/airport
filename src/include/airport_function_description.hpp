#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_function_info.hpp"

namespace duckdb
{
  inline void AirportSetTableFunctionParameters(FunctionDescription &description, const TableFunction &function,
                                                 vector<string> positional_names)
  {
    D_ASSERT(positional_names.size() == function.arguments.size());
    description.parameter_types = function.arguments;
    description.parameter_names = std::move(positional_names);
    // duckdb_functions() includes named arguments after positional arguments.
    // Use the function's iteration order so their names align with their types.
    for (const auto &parameter : function.named_parameters)
    {
      description.parameter_names.push_back(parameter.first);
    }
  }
}
