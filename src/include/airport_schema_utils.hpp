#pragma once
#include "airport_extension.hpp"
#include "duckdb.hpp"

namespace duckdb
{
  struct AirportArrowTableSchema
  {
  public:
    void AddColumn(idx_t index, shared_ptr<ArrowType> type, const string &name);
    const arrow_column_map_t &GetColumns() const;

  private:
    arrow_column_map_t arrow_convert_data;
  };

  duckdb::unique_ptr<duckdb::ArrowType> AirportGetArrowType(
      ClientContext &context,
      ArrowSchema &schema_item);

  void AirportExamineSchema(
      ClientContext &context,
      const ArrowSchemaWrapper &schema_root,
      AirportArrowTableSchema *arrow_table,
      vector<LogicalType> *return_types,
      vector<string> *names,
      vector<string> *duckdb_type_names,
      idx_t *rowid_column_index,
      bool skip_rowid_column);

  bool AirportFieldMetadataIsRowId(const char *metadata);

  TableFunctionCatalogEntry &AirportGetTableFunction(DatabaseInstance &db, const string &name);

}