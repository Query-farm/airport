#pragma once

#include "duckdb.hpp"
#include <numeric>
#include <msgpack.hpp>
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"

namespace duckdb
{

  struct AirportAlterBase
  {
    //! Catalog name to alter
    std::string catalog;
    //! Schema name to alter
    std::string schema;
    //! Entry name to alter
    std::string name;
    bool ignore_not_found;

    explicit AirportAlterBase(const AlterInfo &info, const AirportCatalog &catalog) : catalog(catalog.internal_name()),
                                                                                      schema(info.schema),
                                                                                      name(info.name),
                                                                                      ignore_not_found(info.if_not_found == OnEntryNotFound::RETURN_NULL)
    {
    }
  };

  struct AirportAlterTableRenameParameters : AirportAlterBase
  {
    std::string new_table_name;

    explicit AirportAlterTableRenameParameters(const RenameTableInfo &info, const AirportCatalog &catalog) : AirportAlterBase(info, catalog),
                                                                                                             new_table_name(info.new_table_name)
    {
    }

    MSGPACK_DEFINE_MAP(catalog, schema, name, ignore_not_found, new_table_name);
  };

  struct AirportAlterTableRenameColumnParameters : AirportAlterBase
  {
    std::string old_name;
    std::string new_name;

    explicit AirportAlterTableRenameColumnParameters(const RenameColumnInfo &info, const AirportCatalog &catalog) : AirportAlterBase(info, catalog),
                                                                                                                    old_name(info.old_name),
                                                                                                                    new_name(info.new_name)
    {
    }

    MSGPACK_DEFINE_MAP(catalog, schema, name, ignore_not_found, new_name, old_name);
  };

  struct AirportAlterTableAddColumnParameters : AirportAlterBase
  {
    // How to serialize the type of the column, well we want to use a Arrow schema it seems
    // like with just a single field.
    std::string column_schema;
    //    std::string comment;
    //    unordered_map<std::string, std::string> tags;
    bool if_column_not_exists;

    explicit AirportAlterTableAddColumnParameters(const AddColumnInfo &info, const AirportCatalog &catalog, ClientContext &context,
                                                  const std::string &server_location)
        : AirportAlterBase(info, catalog),
          if_column_not_exists(info.if_column_not_exists)
    {
      ArrowSchema send_schema;
      auto client_properties = context.GetClientProperties();
      ArrowConverter::ToArrowSchema(&send_schema,
                                    {info.new_column.Type()},
                                    {info.new_column.Name()},
                                    client_properties);

      std::shared_ptr<arrow::Schema> cpp_schema;

      // Export the C based schema to the C++ one.
      AIRPORT_ASSIGN_OR_RAISE_LOCATION(
          cpp_schema,
          arrow::ImportSchema(&send_schema),
          server_location,
          "ExportSchema");

      // Now serialize the schema
      AIRPORT_ASSIGN_OR_RAISE_LOCATION(
          auto serialized_schema,
          arrow::ipc::SerializeSchema(*cpp_schema, arrow::default_memory_pool()),
          server_location,
          "");

      column_schema = serialized_schema->ToString();
    }

    MSGPACK_DEFINE_MAP(catalog, schema, name, ignore_not_found, if_column_not_exists, column_schema);
  };

  struct AirportAlterTableAddFieldParameters : AirportAlterBase
  {
    // How to serialize the type of the column, well we want to use a Arrow schema it seems
    // like with just a single field.
    std::string column_schema;

    std::vector<std::string> column_path;
    bool if_field_not_exists;

    explicit AirportAlterTableAddFieldParameters(const AddFieldInfo &info, const AirportCatalog &catalog, ClientContext &context,
                                                 const std::string &server_location)
        : AirportAlterBase(info, catalog),
          column_path(info.column_path),
          if_field_not_exists(info.if_field_not_exists)

    {
      ArrowSchema send_schema;
      auto client_properties = context.GetClientProperties();
      ArrowConverter::ToArrowSchema(&send_schema,
                                    {info.new_field.Type()},
                                    {info.new_field.Name()},
                                    client_properties);

      std::shared_ptr<arrow::Schema> cpp_schema;

      // Export the C based schema to the C++ one.
      AIRPORT_ASSIGN_OR_RAISE_LOCATION(
          cpp_schema,
          arrow::ImportSchema(&send_schema),
          server_location,
          "ExportSchema");

      // Now serialize the schema
      AIRPORT_ASSIGN_OR_RAISE_LOCATION(
          auto serialized_schema,
          arrow::ipc::SerializeSchema(*cpp_schema, arrow::default_memory_pool()),
          server_location,
          "");
      column_schema = serialized_schema->ToString();
    }

    MSGPACK_DEFINE_MAP(catalog, schema, name, ignore_not_found, if_field_not_exists, column_schema, column_path);
  };

  struct AirportRemoveTableColumnParameters : AirportAlterBase
  {
    std::string removed_column;
    bool if_column_exists;
    bool cascade;

    explicit AirportRemoveTableColumnParameters(const RemoveColumnInfo &info, const AirportCatalog &catalog) : AirportAlterBase(info, catalog)
    {
      removed_column = info.removed_column;
      if_column_exists = info.if_column_exists;
      cascade = info.cascade;
    }

    MSGPACK_DEFINE_MAP(catalog, schema, name, ignore_not_found, removed_column, if_column_exists, cascade);
  };

  struct AirportAlterTableRemoveFieldParameters : AirportAlterBase
  {
    std::vector<std::string> column_path;
    bool if_column_exists;
    bool cascade;

    explicit AirportAlterTableRemoveFieldParameters(const RemoveFieldInfo &info, const AirportCatalog &catalog)
        : AirportAlterBase(info, catalog),
          column_path(info.column_path),
          if_column_exists(info.if_column_exists),
          cascade(info.cascade)
    {
    }
    MSGPACK_DEFINE_MAP(catalog, schema, name, ignore_not_found, column_path, if_column_exists, cascade);
  };

  struct AirportAlterTableRenameFieldParameters : AirportAlterBase
  {
    //! Path to source field
    std::vector<std::string> column_path;
    //! Column new name
    std::string new_name;

    explicit AirportAlterTableRenameFieldParameters(const RenameFieldInfo &info, const AirportCatalog &catalog) : AirportAlterBase(info, catalog),
                                                                                                                  column_path(info.column_path),
                                                                                                                  new_name(info.new_name)
    {
    }

    MSGPACK_DEFINE_MAP(catalog, schema, name, ignore_not_found, column_path, new_name);
  };

  struct AirportAlterTableRenameTableParameters : AirportAlterBase
  {
    std::string new_table_name;

    explicit AirportAlterTableRenameTableParameters(const RenameTableInfo &info, const AirportCatalog &catalog)
        : AirportAlterBase(info, catalog),
          new_table_name(info.new_table_name)
    {
    }

    MSGPACK_DEFINE_MAP(catalog, schema, name, ignore_not_found, new_table_name);
  };

  struct AirportAlterTableSetDefaultParameters : AirportAlterBase
  {
    std::string column_name;
    std::string expression;

    explicit AirportAlterTableSetDefaultParameters(const SetDefaultInfo &info, const AirportCatalog &catalog)
        : AirportAlterBase(info, catalog),
          column_name(info.column_name),
          expression(info.expression->ToString())
    {
    }

    MSGPACK_DEFINE_MAP(catalog, schema, name, ignore_not_found, column_name, expression);
  };

  struct AirportAlterTableChangeColumnTypeParameters : AirportAlterBase
  {
    std::string column_schema;
    std::string expression;

    explicit AirportAlterTableChangeColumnTypeParameters(const ChangeColumnTypeInfo &info, const AirportCatalog &catalog, ClientContext &context,
                                                         const std::string &server_location)
        : AirportAlterBase(info, catalog),
          expression(info.expression->ToString())
    {

      ArrowSchema send_schema;
      auto client_properties = context.GetClientProperties();
      ArrowConverter::ToArrowSchema(&send_schema,
                                    {info.target_type},
                                    {info.column_name},
                                    client_properties);

      std::shared_ptr<arrow::Schema> cpp_schema;

      // Export the C based schema to the C++ one.
      AIRPORT_ASSIGN_OR_RAISE_LOCATION(
          cpp_schema,
          arrow::ImportSchema(&send_schema),
          server_location,
          "ExportSchema");

      // Now serialize the schema
      AIRPORT_ASSIGN_OR_RAISE_LOCATION(
          auto serialized_schema,
          arrow::ipc::SerializeSchema(*cpp_schema, arrow::default_memory_pool()),
          server_location,
          "");
      column_schema = serialized_schema->ToString();
    }

    MSGPACK_DEFINE_MAP(catalog, schema, name, ignore_not_found, column_schema, expression);
  };

  struct AirportAlterTableSetNotNullParameters : AirportAlterBase
  {
    std::string column_name;

    explicit AirportAlterTableSetNotNullParameters(const SetNotNullInfo &info, const AirportCatalog &catalog)
        : AirportAlterBase(info, catalog),
          column_name(info.column_name)
    {
    }

    MSGPACK_DEFINE_MAP(catalog, schema, name, ignore_not_found, column_name, ignore_not_found);
  };

  struct AirportAlterTableDropNotNullParameters : AirportAlterBase
  {
    std::string column_name;

    explicit AirportAlterTableDropNotNullParameters(const DropNotNullInfo &info, const AirportCatalog &catalog)
        : AirportAlterBase(info, catalog),
          column_name(info.column_name)
    {
    }

    MSGPACK_DEFINE_MAP(catalog, schema, name, ignore_not_found, column_name);
  };

  struct AirportAlterTableAddConstraintParameters : AirportAlterBase
  {
    std::string constraint;

    explicit AirportAlterTableAddConstraintParameters(const AddConstraintInfo &info, const AirportCatalog &catalog)
        : AirportAlterBase(info, catalog),
          constraint(info.constraint->ToString())
    {
    }

    MSGPACK_DEFINE_MAP(catalog, schema, name, ignore_not_found, constraint);
  };

}
