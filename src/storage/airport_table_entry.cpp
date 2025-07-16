#include "airport_extension.hpp"
#include "storage/airport_catalog.hpp"
#include "storage/airport_schema_entry.hpp"
#include "storage/airport_table_entry.hpp"
#include "storage/airport_transaction.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "storage/airport_catalog_api.hpp"
#include "storage/airport_transaction.hpp"
#include "airport_request_headers.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "storage/airport_alter_parameters.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "airport_schema_utils.hpp"
#include "airport_rpc.hpp"

namespace flight = arrow::flight;

namespace duckdb
{

  AirportTableEntry::AirportTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, const LogicalType &rowid_type)
      : TableCatalogEntry(catalog, schema, info), rowid_type(rowid_type), catalog_(catalog)
  {
    this->internal = false;
  }

  AirportTableEntry::AirportTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, AirportTableInfo &info, const LogicalType &rowid_type)
      : TableCatalogEntry(catalog, schema, *info.create_info), rowid_type(rowid_type), catalog_(catalog)
  {
    this->internal = false;
  }

  unique_ptr<AirportTableEntry> AirportTableEntry::AlterEntryDirect(ClientContext &context, AlterInfo &info)
  {
    D_ASSERT(info.type == AlterType::ALTER_TABLE);
    auto &table_alter = info.Cast<AlterTableInfo>();

    auto &airport_catalog = catalog_.Cast<AirportCatalog>();

    arrow::flight::FlightCallOptions call_options;

    airport_add_standard_headers(call_options, airport_catalog.attach_parameters()->location());
    airport_add_authorization_header(call_options, airport_catalog.attach_parameters()->auth_token());

    auto &server_location = airport_catalog.attach_parameters()->location();
    auto flight_client = AirportAPI::FlightClientForLocation(airport_catalog.attach_parameters()->location());

    // if (alter.type == AlterType::SET_COLUMN_COMMENT)
    // {
    //   auto &comment_on_column_info = info.Cast<SetColumnCommentInfo>();
    //   return SetColumnComment(context, comment_on_column_info);
    // }

    D_ASSERT(table_alter.type == AlterType::ALTER_TABLE);

    if (table_alter.type != AlterType::ALTER_TABLE)
    {
      throw CatalogException("Can only modify table with ALTER TABLE statement");
    }

    auto perform_simple_action = [&](const std::string &action_name, const auto &info, auto &catalog, auto &context, const auto &server_location, auto &&make_params)
    {
      auto params = make_params(info, catalog, context, server_location);
      call_options.headers.emplace_back("airport-action-name", action_name);
      AIRPORT_MSGPACK_ACTION_SINGLE_PARAMETER(action, action_name, params);

      auto result_buffer = AirportCallAction(flight_client, call_options, action, server_location);

      if (result_buffer == nullptr)
      {
        throw AirportFlightException(server_location, "No flight info returned from alter_table action");
      }

      std::string_view serialized_flight_info(reinterpret_cast<const char *>(result_buffer->body->data()), result_buffer->body->size());

      // Now how to we deserialize the flight info from that buffer...
      AIRPORT_ASSIGN_OR_RAISE_LOCATION(
          auto flight_info,
          arrow::flight::FlightInfo::Deserialize(serialized_flight_info),
          server_location,
          "Error deserializing flight info from create_table RPC");

      auto table_entry = AirportCatalogEntryFromFlightInfo(
          std::move(flight_info),
          server_location,
          this->schema,
          catalog,
          context);

      return table_entry;
    };

    switch (table_alter.alter_table_type)
    {
    case AlterTableType::RENAME_COLUMN:
    {
      auto &rename_info = table_alter.Cast<RenameColumnInfo>();
      return perform_simple_action("rename_column", rename_info, airport_catalog, context, server_location, [](const auto &i, auto &catalog, auto &context, const auto &server_location)
                                   { return AirportAlterTableRenameColumnParameters(i, catalog); });
    }
    case AlterTableType::REMOVE_COLUMN:
    {
      auto &remove_info = table_alter.Cast<RemoveColumnInfo>();
      return perform_simple_action("remove_column", remove_info, airport_catalog, context, server_location, [](const auto &i, auto &catalog, auto &context, const auto &server_location)
                                   { return AirportRemoveTableColumnParameters(i, catalog); });
    }
    case AlterTableType::RENAME_FIELD:
    {
      auto &rename_info = table_alter.Cast<RenameFieldInfo>();
      return perform_simple_action("rename_field", rename_info, airport_catalog, context, server_location, [](const auto &i, auto &catalog, auto &context, const auto &server_location)
                                   { return AirportAlterTableRenameFieldParameters(i, catalog); });
    }
    case AlterTableType::RENAME_TABLE:
    {
      auto &rename_info = table_alter.Cast<RenameTableInfo>();
      return perform_simple_action("rename_table", rename_info, airport_catalog, context, server_location, [](const auto &i, auto &catalog, auto &context, const auto &server_location)
                                   { return AirportAlterTableRenameTableParameters(i, catalog); });
    }
    case AlterTableType::ADD_COLUMN:
    {
      auto &add_info = table_alter.Cast<AddColumnInfo>();
      return perform_simple_action("add_column", add_info, airport_catalog, context, server_location, [](const auto &i, auto &catalog, auto &context, const auto &server_location)
                                   { return AirportAlterTableAddColumnParameters(i, catalog, context, server_location); });
    }
    case AlterTableType::ADD_FIELD:
    {
      auto &add_info = table_alter.Cast<AddFieldInfo>();
      return perform_simple_action("add_field", add_info, airport_catalog, context, server_location, [](const auto &i, auto &catalog, auto &context, const auto &server_location)
                                   { return AirportAlterTableAddFieldParameters(i, catalog, context, server_location); });
    }
    case AlterTableType::REMOVE_FIELD:
    {
      auto &remove_info = table_alter.Cast<RemoveFieldInfo>();
      return perform_simple_action("remove_field", remove_info, airport_catalog, context, server_location, [](const auto &i, auto &catalog, auto &context, const auto &server_location)
                                   { return AirportAlterTableRemoveFieldParameters(i, catalog); });
    }
    case AlterTableType::SET_DEFAULT:
    {
      auto &set_default_info = table_alter.Cast<SetDefaultInfo>();
      return perform_simple_action("set_default", set_default_info, airport_catalog, context, server_location, [](const auto &i, auto &catalog, auto &context, const auto &server_location)
                                   { return AirportAlterTableSetDefaultParameters(i, catalog); });
    }
    case AlterTableType::ALTER_COLUMN_TYPE:
    {
      auto &change_type_info = table_alter.Cast<ChangeColumnTypeInfo>();
      return perform_simple_action("change_column_type", change_type_info, airport_catalog, context, server_location, [](const auto &i, auto &catalog, auto &context, const auto &server_location)
                                   { return AirportAlterTableChangeColumnTypeParameters(i, catalog, context, server_location); });
    }
    case AlterTableType::FOREIGN_KEY_CONSTRAINT:
    {
      throw NotImplementedException("FOREIGN KEY CONSTRAINT is not supported for Airport tables");
      // auto &foreign_key_constraint_info = alter.Cast<AlterForeignKeyInfo>();
      // if (foreign_key_constraint_info.type == AlterForeignKeyType::AFT_ADD)
      // {
      //   return AddForeignKeyConstraint(context, foreign_key_constraint_info);
      // }
      // else
      // {
      //   return DropForeignKeyConstraint(context, foreign_key_constraint_info);
      // }
    }
    case AlterTableType::SET_NOT_NULL:
    {
      auto &set_not_null_info = table_alter.Cast<SetNotNullInfo>();

      return perform_simple_action("set_not_null", set_not_null_info, airport_catalog, context, server_location, [](const auto &i, auto &catalog, auto &context, const auto &server_location)
                                   { return AirportAlterTableSetNotNullParameters(i, catalog); });
    }
    case AlterTableType::DROP_NOT_NULL:
    {
      auto &drop_not_null_info = table_alter.Cast<DropNotNullInfo>();
      return perform_simple_action("drop_not_null", drop_not_null_info, airport_catalog, context, server_location, [](const auto &i, auto &catalog, auto &context, const auto &server_location)
                                   { return AirportAlterTableDropNotNullParameters(i, catalog); });
    }
    case AlterTableType::ADD_CONSTRAINT:
    {
      auto &add_constraint_info = table_alter.Cast<AddConstraintInfo>();

      return perform_simple_action("add_constraint", add_constraint_info, airport_catalog, context, server_location, [](const auto &i, auto &catalog, auto &context, const auto &server_location)
                                   { return AirportAlterTableAddConstraintParameters(i, catalog); });
    }
    case AlterTableType::SET_PARTITIONED_BY:
      throw NotImplementedException("SET PARTITIONED BY is not supported for Airport tables");
    case AlterTableType::SET_SORTED_BY:
      throw NotImplementedException("SET SORTED BY is not supported for Airport tables");
    default:
      throw NotImplementedException("Unrecognized alter table type!");
    }
  }

  unique_ptr<BaseStatistics> AirportTableEntry::GetStatistics(ClientContext &context, column_t column_id)
  {
    // TODO: Rusty implement this from the flight server.
    // printf("Getting column statistics for column %d\n", column_id);
    return nullptr;
  }

  TableFunction AirportTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data)
  {
    throw InternalException("AirportTableEntry::GetScanFunction called without entry lookup info");
  }

  TableFunction AirportTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data, const EntryLookupInfo &lookup)
  {
    auto &db = DatabaseInstance::GetDatabase(context);
    auto &airport_take_flight_function_set = ExtensionUtil::GetTableFunction(db, "airport_take_flight");
    auto airport_take_flight_function = airport_take_flight_function_set.functions.GetFunctionByArguments(
        context,
        {LogicalType::POINTER, LogicalType::POINTER, LogicalType::VARCHAR});

    D_ASSERT(table_data);

    auto &transaction = AirportTransaction::Get(context, catalog_);

    // Rusty: this is the place where the transformation happens between table functions and tables.
    vector<Value> inputs = {
        Value::POINTER((uintptr_t)table_data.get()),
        Value::POINTER((uintptr_t)this),
        transaction.identifier().has_value() ? transaction.identifier().value() : ""};

    named_parameter_map_t param_map;
    vector<LogicalType> return_types;
    vector<string> names;
    TableFunctionRef empty_ref;

    auto at = lookup.GetAtClause();
    if (at)
    {
      param_map["at_unit"] = at->Unit();
      param_map["at_value"] = at->GetValue();
    }

    TableFunctionBindInput bind_input(inputs,
                                      param_map,
                                      return_types,
                                      names,
                                      nullptr,
                                      nullptr,
                                      airport_take_flight_function,
                                      empty_ref);

    auto result = airport_take_flight_function.bind(context, bind_input, return_types, names);
    bind_data = std::move(result);

    return airport_take_flight_function;
  }

  TableStorageInfo AirportTableEntry::GetStorageInfo(ClientContext &context)
  {
    TableStorageInfo result;
    // TODO fill info
    return result;
  }

} // namespace duckdb
