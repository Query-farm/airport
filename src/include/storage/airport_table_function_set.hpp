#pragma once

#include "storage/airport_catalog_set.hpp"
#include "storage/airport_catalog_set_base.hpp"

namespace duckdb
{
  class AirportCatalog;

  class AirportTableFunctionSet : public AirportCatalogSetBase
  {

  protected:
    void LoadEntries(DatabaseInstance &db) override;

  public:
    explicit AirportTableFunctionSet(AirportSchemaEntry &schema, const string &cache_directory) : AirportCatalogSetBase(schema, cache_directory)
    {
    }
    ~AirportTableFunctionSet() {}

    // Create a catch-all passthrough function entry for any unregistered function name.
    optional_ptr<CatalogEntry> CreatePassthroughEntry(ClientContext &context, const string &fn_name, AirportCatalog &airport_catalog);

  private:
    // Cache of dynamically-created passthrough entries so we don't recreate on every lookup.
    case_insensitive_map_t<unique_ptr<StandardEntry>> passthrough_entries_;
  };

} // namespace duckdb
