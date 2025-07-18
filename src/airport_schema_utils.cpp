#include "airport_extension.hpp"
#include "duckdb.hpp"

#include "duckdb/common/arrow/schema_metadata.hpp"
#include "airport_take_flight.hpp"

namespace duckdb
{

  duckdb::unique_ptr<duckdb::ArrowType> AirportGetArrowType(
      duckdb::DBConfig &config,
      ArrowSchema &schema_item)
  {
    auto arrow_type = ArrowType::GetArrowLogicalType(config, schema_item);

    if (schema_item.dictionary)
    {
      auto dictionary_type = ArrowType::GetArrowLogicalType(config, *schema_item.dictionary);
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
      ArrowTableType *arrow_table,
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

    auto &config = DBConfig::GetConfig(context);

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

      auto arrow_type = AirportGetArrowType(config, schema);

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
        arrow_table->AddColumn(column_id, std::move(arrow_type));
      }
    }
    QueryResult::DeduplicateColumns(*names);
  }

}