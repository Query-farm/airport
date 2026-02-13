#include "airport_extension.hpp"
#include "duckdb.hpp"

#include "duckdb/common/arrow/schema_metadata.hpp"
#include "airport_take_flight.hpp"

namespace duckdb
{

  duckdb::unique_ptr<duckdb::ArrowType> AirportGetArrowType(
      ClientContext &context,
      ArrowSchema &schema_item)
  {
    auto arrow_type = ArrowType::GetArrowLogicalType(context, schema_item);

    if (schema_item.dictionary)
    {
      auto dictionary_type = ArrowType::GetArrowLogicalType(context, *schema_item.dictionary);
      arrow_type->SetDictionary(std::move(dictionary_type));
    }
    return arrow_type;
  }

  bool AirportFieldMetadataIsRowId(const char *metadata)
  {
    if (metadata == nullptr)
    {
      return false;
    }
    ArrowSchemaMetadata column_metadata(metadata);
    auto comment = column_metadata.GetOption("is_rowid");
    if (!comment.empty())
    {
      return true;
    }
    return false;
  }

  void AirportExamineSchema(
      ClientContext &context,
      const ArrowSchemaWrapper &schema_root,
      AirportArrowTableSchema *arrow_table,
      vector<LogicalType> *return_types,
      vector<string> *names,
      vector<string> *duckdb_type_names,
      idx_t *rowid_column_index,
      bool skip_rowid_column)
  {
    if (rowid_column_index)
    {
      *rowid_column_index = COLUMN_IDENTIFIER_ROW_ID;
    }

    const idx_t num_columns = static_cast<idx_t>(schema_root.arrow_schema.n_children);

    if (num_columns > 0)
    {
      if (return_types)
      {
        return_types->reserve(num_columns);
      }
      if (names)
      {
        names->reserve(num_columns);
      }
      if (duckdb_type_names)
      {
        duckdb_type_names->reserve(num_columns);
      }
    }

    for (idx_t col_idx = 0; col_idx < num_columns; col_idx++)
    {
      auto &schema = *schema_root.arrow_schema.children[col_idx];
      if (!schema.release)
      {
        throw InvalidInputException("AirportExamineSchema: released schema passed");
      }

      // Determine if the column is the rowid column by looking at the metadata
      // on the column.
      bool is_rowid_column = false;
      if (AirportFieldMetadataIsRowId(schema.metadata))
      {
        is_rowid_column = true;
        if (rowid_column_index)
        {
          *rowid_column_index = col_idx;
        }
      }

      auto arrow_type = AirportGetArrowType(context, schema);

      const idx_t column_id = is_rowid_column ? COLUMN_IDENTIFIER_ROW_ID : col_idx;

      const string column_name = AirportNameForField(schema.name, col_idx);

      if (!skip_rowid_column || !is_rowid_column)
      {
        auto duck_type = arrow_type->GetDuckType();
        if (return_types)
        {
          return_types->push_back(duck_type);
        }
        if (names)
        {
          names->push_back(std::move(column_name));
        }
        if (duckdb_type_names)
        {
          duckdb_type_names->push_back(duck_type.ToString());
        }
      }

      if (arrow_table)
      {
        arrow_table->AddColumn(column_id, std::move(arrow_type), schema.name);
      }
    }
    QueryResult::DeduplicateColumns(*names);
  }

  static optional_ptr<CatalogEntry> TryGetEntry(DatabaseInstance &db, const string &name, CatalogType type)
  {
    D_ASSERT(!name.empty());
    auto &system_catalog = Catalog::GetSystemCatalog(db);
    auto data = CatalogTransaction::GetSystemTransaction(db);
    auto &schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);
    return schema.GetEntry(data, type, name);
  }

  TableFunctionCatalogEntry &AirportGetTableFunction(DatabaseInstance &db, const string &name)
  {
    auto catalog_entry = TryGetEntry(db, name, CatalogType::TABLE_FUNCTION_ENTRY);

    if (!catalog_entry)
    {
      throw InvalidInputException("Function with name \"%s\" not found, check to see if all necessary extensions are loaded.", name);
    }
    return catalog_entry->Cast<TableFunctionCatalogEntry>();
  }

  void AirportArrowTableSchema::AddColumn(idx_t index, shared_ptr<ArrowType> type, const string &name)
  {
    D_ASSERT(arrow_convert_data.find(index) == arrow_convert_data.end());
    arrow_convert_data.emplace(std::make_pair(index, std::move(type)));
  }

  const arrow_column_map_t &AirportArrowTableSchema::GetColumns() const
  {
    return arrow_convert_data;
  }

}