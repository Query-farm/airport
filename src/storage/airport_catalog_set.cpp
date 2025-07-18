#include "storage/airport_catalog_set.hpp"
#include "storage/airport_transaction.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "storage/airport_schema_entry.hpp"
#include "airport_request_headers.hpp"
#include "storage/airport_catalog.hpp"
#include "airport_macros.hpp"
#include <arrow/buffer.h>
#include <msgpack.hpp>
#include "airport_rpc.hpp"

namespace duckdb
{

  AirportCatalogSet::AirportCatalogSet(Catalog &catalog) : catalog(catalog), is_loaded(false)
  {
  }

  optional_ptr<CatalogEntry> AirportCatalogSet::GetEntry(ClientContext &context, const EntryLookupInfo &lookup_info)
  {
    lock_guard<mutex> l(entry_lock);
    if (!is_loaded)
    {
      is_loaded = true;
      LoadEntries(context);
    }
    auto entry = entries.find(lookup_info.GetEntryName());
    if (entry == entries.end())
    {
      return nullptr;
    }
    return entry->second.get();
  }

  struct DropItemActionParameters
  {
    // the type of the item to drop, table, schema.
    std::string type;

    std::string catalog_name;
    std::string schema_name;
    std::string name;

    bool ignore_not_found;

    MSGPACK_DEFINE_MAP(type, catalog_name, schema_name, name, ignore_not_found)
  };

  void AirportCatalogSet::DropEntry(ClientContext &context, DropInfo &info)
  {
    if (!is_loaded)
    {
      is_loaded = true;
      LoadEntries(context);
    }

    auto &airport_catalog = catalog.Cast<AirportCatalog>();
    arrow::flight::FlightCallOptions call_options;

    airport_add_standard_headers(call_options, airport_catalog.attach_parameters()->location());
    airport_add_authorization_header(call_options, airport_catalog.attach_parameters()->auth_token());

    auto flight_client = AirportAPI::FlightClientForLocation(airport_catalog.attach_parameters()->location());

    // Common parameters
    DropItemActionParameters params;
    params.catalog_name = airport_catalog.internal_name();
    params.schema_name = info.schema;
    params.name = info.name;
    params.ignore_not_found = (info.if_not_found == OnEntryNotFound::RETURN_NULL) ? true : false;

    std::string action_type;

    switch (info.type)
    {
    case CatalogType::TABLE_ENTRY:
      params.type = "table";
      action_type = "drop_table";
      break;
    case CatalogType::SCHEMA_ENTRY:
      params.type = "schema";
      action_type = "drop_schema";
      break;
    default:
      throw NotImplementedException("AirportCatalogSet::DropEntry for type");
    }

    AIRPORT_MSGPACK_ACTION_SINGLE_PARAMETER(action, action_type, params);

    auto &server_location = airport_catalog.attach_parameters()->location();

    AirportCallAction(flight_client, call_options, action, server_location, false);

    D_ASSERT(entries.find(info.name) != entries.end());

    EraseEntryInternal(info.name);
  }

  void AirportCatalogSet::EraseEntryInternal(const string &name)
  {
    lock_guard<mutex> l(entry_lock);
    entries.erase(name);
  }

  void AirportCatalogSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback)
  {
    lock_guard<mutex> l(entry_lock);
    if (!is_loaded)
    {
      is_loaded = true;
      LoadEntries(context);
    }
    for (auto &entry : entries)
    {
      callback(*entry.second);
    }
  }

  optional_ptr<CatalogEntry> AirportCatalogSet::CreateEntry(unique_ptr<CatalogEntry> entry)
  {
    auto result = entry.get();
    if (result->name.empty())
    {
      throw CatalogException("AirportCatalogSet::CreateEntry called with empty name");
    }
    //    printf("Creating catalog entry\n");
    entries.insert(make_pair(result->name, std::move(entry)));
    return result;
  }

  void AirportCatalogSet::ClearEntries()
  {
    lock_guard<mutex> l(entry_lock);
    entries.clear();
    is_loaded = false;
  }

  void AirportCatalogSet::ReplaceEntry(
      const string &name,
      unique_ptr<CatalogEntry> entry)
  {
    lock_guard<mutex> l(entry_lock);
    auto it = entries.find(name);
    if (it == entries.end())
    {
      throw CatalogException("AirportCatalogSet::ReplaceEntry called with non-existing entry");
    }

    if (entry->name == name)
    {
      it->second = std::move(entry);
    }
    else
    {
      entries.erase(name);
      CreateEntry(std::move(entry));
    }
  }

  AirportInSchemaSet::AirportInSchemaSet(AirportSchemaEntry &schema) : AirportCatalogSet(schema.ParentCatalog()), schema(schema)
  {
  }

  optional_ptr<CatalogEntry> AirportInSchemaSet::CreateEntry(unique_ptr<CatalogEntry> entry)
  {
    if (!entry->internal)
    {
      entry->internal = schema.internal;
    }
    return AirportCatalogSet::CreateEntry(std::move(entry));
  }

} // namespace duckdb
