#pragma once
#include "airport_extension.hpp"
#include "duckdb.hpp"

namespace duckdb
{

  duckdb::unique_ptr<duckdb::ArrowType> AirportGetArrowType(
      duckdb::DBConfig &config,
      ArrowSchema &schema_item);

  void AirportExamineSchema(
      ClientContext &context,
      const ArrowSchemaWrapper &schema_root,
      ArrowTableType *arrow_table,
      vector<LogicalType> *return_types,
      vector<string> *names,
      vector<string> *duckdb_type_names,
      idx_t *rowid_column_index,
      bool skip_rowid_column);

  bool AirportFieldMetadataIsRowId(const char *metadata);
}