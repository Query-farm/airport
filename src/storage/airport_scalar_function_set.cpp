#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/schema_metadata.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/parser/constraints/check_constraint.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include <arrow/buffer.h>
#include <arrow/c/bridge.h>
#include <arrow/io/memory.h>
#include <arrow/util/key_value_metadata.h>
#include <numeric>
#include "airport_flight_stream.hpp"
#include "airport_request_headers.hpp"
#include "airport_macros.hpp"
#include "airport_macros.hpp"
#include "airport_scalar_function.hpp"
#include "airport_secrets.hpp"
#include "airport_take_flight.hpp"
#include "storage/airport_catalog_api.hpp"
#include "storage/airport_catalog.hpp"
#include "storage/airport_exchange.hpp"
#include "storage/airport_schema_entry.hpp"
#include "storage/airport_table_set.hpp"
#include "storage/airport_transaction.hpp"
#include "airport_schema_utils.hpp"
#include "storage/airport_alter_parameters.hpp"
#include "storage/airport_scalar_function_set.hpp"

namespace duckdb
{

  // Given an Arrow schema return a vector of the LogicalTypes for that schema.
  static vector<LogicalType> AirportSchemaToLogicalTypes(
      ClientContext &context,
      std::shared_ptr<arrow::Schema> schema,
      const AirportLocationDescriptor &location_descriptor)
  {
    ArrowSchemaWrapper schema_root;

    AIRPORT_ARROW_ASSERT_OK_CONTAINER(
        ExportSchema(*schema, &schema_root.arrow_schema),
        &location_descriptor,
        "ExportSchema");

    vector<LogicalType> return_types;
    auto &config = DBConfig::GetConfig(context);

    const idx_t column_count = (idx_t)schema_root.arrow_schema.n_children;

    return_types.reserve(column_count);

    for (idx_t col_idx = 0;
         col_idx < column_count; col_idx++)
    {
      auto &schema_item = *schema_root.arrow_schema.children[col_idx];
      if (!schema_item.release)
      {
        throw InvalidInputException("AirportSchemaToLogicalTypes: released schema passed");
      }
      auto arrow_type = AirportGetArrowType(config, schema_item);

      // Indicate that the field should select any type.
      bool is_any_type = false;
      if (schema_item.metadata != nullptr)
      {
        auto column_metadata = ArrowSchemaMetadata(schema_item.metadata);
        if (!column_metadata.GetOption("is_any_type").empty())
        {
          is_any_type = true;
        }
      }

      if (is_any_type)
      {
        // This will be sorted out in the bind of the function.
        return_types.push_back(LogicalType::ANY);
      }
      else
      {
        return_types.emplace_back(arrow_type->GetDuckType());
      }
    }
    return return_types;
  }

  void AirportScalarFunctionSet::LoadEntries(ClientContext &context)
  {
    // auto &transaction = AirportTransaction::Get(context, catalog);

    auto &airport_catalog = catalog.Cast<AirportCatalog>();

    // TODO: handle out-of-order columns using position property
    auto contents = AirportAPI::GetSchemaItems(
        context,
        catalog.GetDBPath(),
        schema.name,
        schema.serialized_source(),
        cache_directory_,
        airport_catalog.attach_parameters());

    //    printf("AirportScalarFunctionSet loading entries\n");
    //    printf("Total functions: %lu\n", tables_and_functions.second.size());

    // There can be functions with the same name.
    std::unordered_map<AirportFunctionCatalogSchemaNameKey, std::vector<AirportAPIScalarFunction>> functions_by_name;

    for (auto &function : contents->scalar_functions)
    {
      AirportFunctionCatalogSchemaNameKey function_key{function.catalog_name(), function.schema_name(), function.name()};
      functions_by_name[function_key].emplace_back(function);
    }

    for (const auto &pair : functions_by_name)
    {
      ScalarFunctionSet flight_func_set(pair.first.name);

      // FIXME: need a way to specify the function stability.
      for (const auto &function : pair.second)
      {
        auto input_types = AirportSchemaToLogicalTypes(context, function.input_schema(), function);

        auto output_types = AirportSchemaToLogicalTypes(context, function.schema(), function);
        D_ASSERT(output_types.size() == 1);

        auto scalar_func = ScalarFunction(input_types, output_types[0],
                                          AirportScalarFunctionProcessChunk,
                                          AirportScalarFunctionBind,
                                          nullptr,
                                          nullptr,
                                          AirportScalarFunctionInitLocalState,
                                          LogicalTypeId::INVALID,
                                          function.stability(),
                                          duckdb::FunctionNullHandling::DEFAULT_NULL_HANDLING,
                                          nullptr);
        scalar_func.function_info = make_uniq<AirportScalarFunctionInfo>(function.name(),
                                                                         function,
                                                                         function.schema(),
                                                                         function.input_schema(),
                                                                         catalog);

        flight_func_set.AddFunction(scalar_func);
      }

      CreateScalarFunctionInfo info = CreateScalarFunctionInfo(flight_func_set);
      info.catalog = pair.first.catalog_name;
      info.schema = pair.first.schema_name;

      info.internal = true;

      auto function_entry = make_uniq_base<StandardEntry, ScalarFunctionCatalogEntry>(
          catalog,
          schema,
          info.Cast<CreateScalarFunctionInfo>());

      CreateEntry(std::move(function_entry));
    }
  }

}