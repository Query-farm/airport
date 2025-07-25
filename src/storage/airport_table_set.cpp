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
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "airport_rpc.hpp"

namespace duckdb
{

  struct AirportTableCheckConstraints
  {
    std::vector<std::string> constraints;

    MSGPACK_DEFINE_MAP(constraints)
  };

  void AirportArrowSchemaToCreateTableInfo(CreateTableInfo &info,
                                           std::shared_ptr<arrow::Schema> info_schema,
                                           ClientContext &context,
                                           const std::string &table_name,
                                           AirportLocationDescriptor &location,
                                           LogicalType &rowid_type)
  {
    auto &config = DBConfig::GetConfig(context);

    ArrowSchema arrow_schema;

    AIRPORT_ARROW_ASSERT_OK_CONTAINER(ExportSchema(*info_schema, &arrow_schema),
                                      &location, "ExportSchema");

    vector<string> column_names;
    vector<duckdb::LogicalType> return_types;
    vector<string> not_null_columns;

    rowid_type = LogicalType(LogicalType::SQLNULL);

    if (arrow_schema.metadata != nullptr)
    {
      auto schema_metadata = ArrowSchemaMetadata(arrow_schema.metadata);

      auto check_constraints = schema_metadata.GetOption("check_constraints");
      if (!check_constraints.empty())
      {
        AirportTableCheckConstraints table_constraints;

        AIRPORT_MSGPACK_UNPACK_CONTAINER(
            table_constraints,
            check_constraints,
            (&location),
            "File to parse msgpack encoded table check constraints.");

        for (auto &expression : table_constraints.constraints)
        {
          auto expression_list = Parser::ParseExpressionList(expression, context.GetParserOptions());
          if (expression_list.size() != 1)
          {
            throw ParserException("Failed to parse CHECK constraint expression: " + expression + " for table " + table_name);
          }
          info.constraints.push_back(make_uniq<CheckConstraint>(std::move(expression_list[0])));
        }
      }
    }

    for (idx_t col_idx = 0;
         col_idx < (idx_t)arrow_schema.n_children; col_idx++)
    {
      auto &column = *arrow_schema.children[col_idx];
      if (!column.release)
      {
        throw InvalidInputException("ParseArrowSchemaIntoTableInfo released schema passed");
      }

      if (AirportFieldMetadataIsRowId(column.metadata))
      {
        rowid_type = AirportGetArrowType(config, column)->GetDuckType();

        // So the skipping here is a problem, since its assumed
        // that the return_type and column_names can be easily indexed.
        continue;
      }

      auto column_name = AirportNameForField(column.name, col_idx);

      column_names.emplace_back(column_name);

      auto arrow_type = ArrowType::GetArrowLogicalType(config, column);
      if (column.dictionary)
      {
        auto dictionary_type = ArrowType::GetArrowLogicalType(config, *column.dictionary);
        return_types.emplace_back(dictionary_type->GetDuckType());
      }
      else
      {
        return_types.emplace_back(arrow_type->GetDuckType());
      }

      if (!(column.flags & ARROW_FLAG_NULLABLE))
      {
        not_null_columns.emplace_back(column_name);
      }
    }

    QueryResult::DeduplicateColumns(column_names);
    idx_t rowid_adjust = 0;
    for (idx_t col_idx = 0;
         col_idx < (idx_t)arrow_schema.n_children; col_idx++)
    {
      auto &column = *arrow_schema.children[col_idx];
      if (AirportFieldMetadataIsRowId(column.metadata))
      {
        rowid_adjust = 1;
        continue;
      }

      auto column_def = ColumnDefinition(column_names[col_idx - rowid_adjust], return_types[col_idx - rowid_adjust]);
      if (column.metadata != nullptr)
      {
        auto column_metadata = ArrowSchemaMetadata(column.metadata);

        auto comment = column_metadata.GetOption("comment");
        if (!comment.empty())
        {
          column_def.SetComment(duckdb::Value(comment));
        }

        auto default_value = column_metadata.GetOption("default");

        if (!default_value.empty())
        {
          auto expressions = Parser::ParseExpressionList(default_value);
          if (expressions.empty())
          {
            throw AirportFlightException(location.server_location(), location.descriptor(), "Expression list is empty when parsing default value for column", column.name);
          }
          column_def.SetDefaultValue(std::move(expressions[0]));
        }
      }

      info.columns.AddColumn(std::move(column_def));
    }
    arrow_schema.release(&arrow_schema);
    info.columns.Finalize();

    for (auto col_name : not_null_columns)
    {
      auto not_null_index = info.columns.GetColumnIndex(col_name);
      info.constraints.emplace_back(make_uniq<NotNullConstraint>(not_null_index));
    }
  }

  void AirportTableSet::LoadEntries(ClientContext &context)
  {
    // auto &transaction = AirportTransaction::Get(context, catalog);
    auto &airport_catalog = catalog.Cast<AirportCatalog>();

    auto contents = AirportAPI::GetSchemaItems(
        context,
        catalog.GetDBPath(),
        schema.name,
        schema.serialized_source(),
        cache_directory_,
        airport_catalog.attach_parameters());

    for (auto &table : contents->tables)
    {
      // D_ASSERT(schema.name == table.schema_name);
      CreateTableInfo info;

      info.table = table.name();
      info.comment = table.comment().value_or("");

      LogicalType rowid_type;
      AirportArrowSchemaToCreateTableInfo(info,
                                          table.schema(),
                                          context,
                                          table.name(),
                                          table,
                                          rowid_type);

      auto table_entry = make_uniq<AirportTableEntry>(catalog, schema, info, rowid_type);
      table_entry->table_data = make_uniq<AirportAPITable>(table);
      CreateEntry(std::move(table_entry));
    }
  }

  optional_ptr<CatalogEntry> AirportTableSet::RefreshTable(ClientContext &context, const string &table_name)
  {
    throw NotImplementedException("AirportTableSet::RefreshTable");
    // auto table_info = GetTableInfo(context, schema, table_name);
    // auto table_entry = make_uniq<AirportTableEntry>(catalog, schema, *table_info);
    // auto table_ptr = table_entry.get();
    // CreateEntry(std::move(table_entry));
    // return table_ptr;
  }

  unique_ptr<AirportTableInfo> AirportTableSet::GetTableInfo(ClientContext &context, AirportSchemaEntry &schema,
                                                             const string &table_name)
  {
    throw NotImplementedException("AirportTableSet::GetTableInfo");
  }

  struct AirportCreateTableParameters
  {
    std::string catalog_name;
    std::string schema_name;
    std::string table_name;

    // The serialized Arrow schema for the table.
    std::string arrow_schema;

    // This will be "error", "ignore", or "replace"
    std::string on_conflict;

    // The list of constraint expressions.
    std::vector<uint64_t> not_null_constraints;
    std::vector<uint64_t> unique_constraints;
    std::vector<std::string> check_constraints;

    std::vector<std::string> primary_key_columns;
    std::vector<std::string> unique_columns;

    unordered_set<std::string> multi_key_primary_keys;
    std::vector<std::string> extra_constraints;

    MSGPACK_DEFINE_MAP(catalog_name, schema_name, table_name, arrow_schema, on_conflict,
                       not_null_constraints, unique_constraints, check_constraints, primary_key_columns,
                       unique_columns, multi_key_primary_keys, extra_constraints)
  };

  unique_ptr<AirportTableEntry> AirportCatalogEntryFromFlightInfo(
      std::unique_ptr<arrow::flight::FlightInfo> flight_info,
      const std::string &server_location,
      SchemaCatalogEntry &schema_entry,
      Catalog &catalog,
      ClientContext &context)
  {
    if (flight_info->app_metadata().empty())
    {
      throw AirportFlightException(server_location, flight_info->descriptor(), "The app_metadata field of the flight is empty.", "");
    }

    AirportSerializedFlightAppMetadata app_metadata_obj;
    AIRPORT_MSGPACK_UNPACK(app_metadata_obj,
                           flight_info->app_metadata(),
                           server_location,
                           "Failed to unpack flight's app_metadata");

    AirportLocationDescriptor table_location(
        server_location,
        flight_info->descriptor());

    std::shared_ptr<arrow::Schema> info_schema;
    arrow::ipc::DictionaryMemo dictionary_memo;
    AIRPORT_ASSIGN_OR_RAISE_CONTAINER(info_schema,
                                      flight_info->GetSchema(&dictionary_memo),
                                      &table_location,
                                      "");

    // Its important to use the schema returned from the server, not CreateTableInfo
    // that was converted to be sent to the server.
    CreateTableInfo create_info;
    LogicalType rowid_type;
    create_info.table = app_metadata_obj.name;
    create_info.comment = app_metadata_obj.comment.value_or("");
    create_info.schema = app_metadata_obj.schema;
    create_info.catalog = app_metadata_obj.catalog;

    AirportArrowSchemaToCreateTableInfo(create_info,
                                        info_schema,
                                        context,
                                        app_metadata_obj.name,
                                        table_location,
                                        rowid_type);

    auto table_entry = make_uniq<AirportTableEntry>(
        catalog, schema_entry, create_info, rowid_type);

    // Since we're only reading a schema from the server, and we don't have the full
    // table metadata from the server (from list_schemas), we're going to fake this for now.
    AirportSerializedFlightAppMetadata created_table_metadata;
    created_table_metadata.catalog = app_metadata_obj.catalog;
    created_table_metadata.schema = app_metadata_obj.schema;
    created_table_metadata.name = app_metadata_obj.name;
    created_table_metadata.comment = app_metadata_obj.comment;

    table_entry->table_data = make_uniq<AirportAPITable>(table_location,
                                                         AirportAPIObjectBase::GetSchema(server_location, *flight_info),
                                                         created_table_metadata);
    return table_entry;
  }

  optional_ptr<CatalogEntry> AirportTableSet::CreateTable(ClientContext &context, BoundCreateTableInfo &info)
  {
    auto &airport_catalog = catalog.Cast<AirportCatalog>();
    auto &base = info.base->Cast<CreateTableInfo>();

    // Take the information from the CreateTableInfo and create an Arrow schema
    // from it so it can be serialized and sent ot the server.
    vector<LogicalType> column_types;
    vector<string> column_names;
    for (auto &col : base.columns.Logical())
    {
      column_types.push_back(col.GetType());
      column_names.push_back(col.Name());
    }

    ArrowSchema schema;
    auto client_properties = context.GetClientProperties();
    auto &server_location = airport_catalog.attach_parameters()->location();

    // This will create a C Arrow Schema (since this is DuckDB that handles
    // all of the DuckDB types),
    // that schema needs to be exported to C++
    // so it can be serialized and sent to the server.
    ArrowConverter::ToArrowSchema(&schema,
                                  column_types,
                                  column_names,
                                  client_properties);

    AIRPORT_ASSIGN_OR_RAISE_LOCATION(auto real_schema, arrow::ImportSchema(&schema), server_location, "");

    AIRPORT_ASSIGN_OR_RAISE_LOCATION(
        auto serialized_schema,
        arrow::ipc::SerializeSchema(*real_schema, arrow::default_memory_pool()),
        server_location,
        "serialize schema");

    AirportCreateTableParameters params;
    params.catalog_name = airport_catalog.internal_name();
    params.schema_name = base.schema;
    params.table_name = base.table;
    params.arrow_schema = serialized_schema->ToString();
    switch (info.base->on_conflict)
    {
    case OnCreateConflict::ERROR_ON_CONFLICT:
      params.on_conflict = "error";
      break;
    case OnCreateConflict::IGNORE_ON_CONFLICT:
      params.on_conflict = "ignore";
      break;
    case OnCreateConflict::REPLACE_ON_CONFLICT:
      params.on_conflict = "replace";
      break;
    default:
      throw NotImplementedException("Unimplemented conflict type");
    }

    for (auto &c : base.constraints)
    {
      if (c->type == ConstraintType::NOT_NULL)
      {
        auto not_null_constraint = reinterpret_cast<NotNullConstraint *>(c.get());
        params.not_null_constraints.push_back(not_null_constraint->index.index);
      }
      else if (c->type == ConstraintType::UNIQUE)
      {
        auto &pk = c->Cast<UniqueConstraint>();
        if (pk.HasIndex())
        {
          // Single column is specified.
          if (pk.IsPrimaryKey())
          {
            params.primary_key_columns.push_back(pk.GetColumnNames()[0]);
          }
          else
          {
            for (auto &col : pk.GetColumnNames())
            {
              params.unique_columns.push_back(col);
            }
          }
        }
        else
        {
          // multi-column constraint
          if (pk.IsPrimaryKey())
          {
            // multi key pk column: insert set of columns into multi_key_pks
            for (auto &col : pk.GetColumnNames())
            {
              params.multi_key_primary_keys.insert(col);
            }
          }
          else
          {
            params.extra_constraints.push_back(pk.ToString());
          }
        }
      }
      else if (c->type == ConstraintType::CHECK)
      {
        auto check_constraint = reinterpret_cast<CheckConstraint *>(c.get());
        params.check_constraints.push_back(check_constraint->expression->ToString());
      }
    }

    arrow::flight::FlightCallOptions call_options;

    airport_add_standard_headers(call_options, airport_catalog.attach_parameters()->location());
    airport_add_authorization_header(call_options, airport_catalog.attach_parameters()->auth_token());

    auto flight_client = AirportAPI::FlightClientForLocation(airport_catalog.attach_parameters()->location());

    AIRPORT_MSGPACK_ACTION_SINGLE_PARAMETER(action, "create_table", params);

    auto result_buffer = AirportCallAction(flight_client, call_options, action, server_location);

    if (result_buffer == nullptr)
    {
      throw AirportFlightException(server_location, "No flight info returned from create_table action");
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

    return CreateEntry(std::move(table_entry));
  }

  void AirportTableSet::AlterTable(ClientContext &context, AlterTableInfo &alter)
  {
    //    auto &transaction = AirportTransaction::Get(context, catalog);
    //  auto &airport_catalog = catalog.Cast<AirportCatalog>();
    EntryLookupInfo lookup_info(CatalogType::TABLE_ENTRY, alter.name);
    auto entry = GetEntry(context, lookup_info);
    if (!entry)
    {
      return;
    }

    auto &airport_entry = entry.get()->Cast<AirportTableEntry>();
    ReplaceEntry(alter.name, airport_entry.AlterEntryDirect(context, alter));
  }

  optional_ptr<CatalogEntry> AirportTableSet::GetEntry(ClientContext &context, const EntryLookupInfo &lookup_info)
  {
    auto existing_entry = AirportCatalogSet::GetEntry(context, lookup_info);

    auto at = lookup_info.GetAtClause();
    // If we aren't doing anything special with point-in-time queries,
    // just return the base class entry.
    if (!at)
    {
      return existing_entry;
    }

    // Otherwise ask the server for the flight info at the point in
    // time of interest.

    if (!existing_entry)
    {
      return nullptr;
    }
    auto &airport_entry = existing_entry->Cast<AirportTableEntry>();

    auto &airport_catalog = airport_entry.catalog.Cast<AirportCatalog>();

    auto client_properties = context.GetClientProperties();
    auto &server_location = airport_catalog.attach_parameters()->location();

    AirportFlightInfoParameters params;

    AIRPORT_ASSIGN_OR_RAISE_LOCATION(
        params.descriptor,
        airport_entry.table_data->descriptor().SerializeToString(),
        server_location,
        "serialize flight descriptor");

    params.at_unit = at->Unit();
    params.at_value = at->GetValue().ToString();

    arrow::flight::FlightCallOptions call_options;

    airport_add_standard_headers(call_options, airport_catalog.attach_parameters()->location());
    airport_add_authorization_header(call_options, airport_catalog.attach_parameters()->auth_token());

    auto flight_client = AirportAPI::FlightClientForLocation(airport_catalog.attach_parameters()->location());

    AIRPORT_MSGPACK_ACTION_SINGLE_PARAMETER(action, "flight_info", params);

    auto result_buffer = AirportCallAction(flight_client, call_options, action, server_location);

    if (result_buffer == nullptr)
    {
      throw AirportFlightException(server_location, "No flight info returned from flight_info action");
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

    // This is really just a temporary catalog entry since its a point
    // in time, it shoudln't be added to the main catalog, but for now
    // just accumulate these entries in the table set.
    auto &transaction = AirportTransaction::Get(context, catalog);

    auto &result = transaction.point_in_time_entries_.emplace_back(std::move(table_entry));

    return result;
  }

} // namespace duckdb
