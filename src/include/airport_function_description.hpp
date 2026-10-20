#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_function_info.hpp"

namespace duckdb
{
  // DuckDB 2.0 replaced TableFunction::named_parameters with typed "**kwargs" options declared on the
  // function signature. Optional named arguments are declared as options, so a call may leave them out.
  inline void AirportAddNamedParameters(TableFunction &function,
                                        const vector<std::pair<string, LogicalType>> &named_parameters)
  {
    auto add_options = [&](TypedKwargs &options)
    {
      for (const auto &parameter : named_parameters)
      {
        options.Add(Identifier(parameter.first), parameter.second);
      }
    };
    auto &signature = function.GetSignature();
    if (signature.GetTypedKwargs())
    {
      signature.ExtendTypedKwargs(add_options);
    }
    else
    {
      signature.WithTypedKwargs("options", add_options);
    }
  }

  // Name the positional parameters of a table function. DuckDB 2.0 reports the signature's parameter
  // names in duckdb_functions(), so this must be done before the function is added to its set.
  inline void AirportNamePositionalParameters(TableFunction &function, const vector<string> &positional_names)
  {
    auto &signature = function.GetSignature();
    D_ASSERT(positional_names.size() == signature.GetPositionalParameterCount());
    for (idx_t i = 0; i < positional_names.size(); i++)
    {
      signature.GetParameter(i).SetName(Identifier(positional_names[i]));
    }
  }

  // Describe the parameters of a table function for duckdb_functions().
  inline void AirportSetTableFunctionParameters(FunctionDescription &description, const TableFunction &function,
                                                 vector<string> positional_names)
  {
    auto &signature = function.GetSignature();
    D_ASSERT(positional_names.size() == signature.GetPositionalParameterCount());
    description.parameter_types.clear();
    for (idx_t i = 0; i < positional_names.size(); i++)
    {
      description.parameter_types.push_back(signature.GetParameter(i).GetType());
    }
    description.parameter_names = std::move(positional_names);
    // duckdb_functions() includes named arguments after positional arguments.
    // Use the signature's option order so their names align with their types.
    auto options = signature.GetTypedKwargs();
    if (options)
    {
      for (const auto &option : options->GetOptions())
      {
        description.parameter_names.push_back(option.name.GetIdentifierName());
      }
    }
  }
}
