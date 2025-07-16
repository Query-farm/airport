#include "airport_extension.hpp"
#include "storage/airport_schema_set.hpp"
#include "storage/airport_catalog.hpp"
#include "storage/airport_transaction.hpp"
#include "storage/airport_catalog_api.hpp"
#include "storage/airport_schema_entry.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/file_system.hpp"

#include "airport_request_headers.hpp"
#include "airport_macros.hpp"
#include <arrow/buffer.h>
#include "msgpack.hpp"
#include "airport_rpc.hpp"

namespace duckdb
{
  // Set the connection pool size.
  AirportSchemaSet::AirportSchemaSet(Catalog &catalog) : AirportCatalogSet(catalog)
  {
  }

  static bool IsInternalTable(const string &catalog, const string &schema)
  {
    if (schema == "information_schema")
    {
      return true;
    }
    return false;
  }

  static string DuckDBHomeDirectory(ClientContext &context)
  {
    auto &fs = FileSystem::GetFileSystem(context);

    string home_directory = fs.GetHomeDirectory();
    // exception if the home directory does not exist, don't create whatever we think is home
    if (!fs.DirectoryExists(home_directory))
    {
      throw IOException("Can't find the home directory at '%s'\nSpecify a home directory using the SET "
                        "home_directory='/path/to/dir' option.",
                        home_directory);
    }
    string cache_path = home_directory;
    cache_path = fs.JoinPath(cache_path, ".duckdb");
    return cache_path;
  }

  void AirportSchemaSet::LoadEntireSet(ClientContext &context)
  {
    lock_guard<mutex> l(entry_lock);

    if (called_load_entries == false)
    {
      // We haven't called load entries yet.
      LoadEntries(context);
      called_load_entries = true;
    }
  }

  void AirportSchemaSet::LoadEntries(ClientContext &context)
  {
    if (called_load_entries)
    {
      return;
    }
    //    printf("Calling LoadEntries on AirportSchemaSet, catalog basically\n");

    auto &airport_catalog = catalog.Cast<AirportCatalog>();
    string cache_path = DuckDBHomeDirectory(context);

    // catalog.GetName() is the catalog name even if its been aliased.
    // airport_catalog.internal_name() is the name of the database as passed ot attach.
    auto returned_collection = AirportAPI::GetSchemas(airport_catalog.internal_name(), airport_catalog.attach_parameters());

    airport_catalog.loaded_catalog_version = returned_collection->version_info;

    collection = std::move(returned_collection);

    std::unordered_set<string> seen_schema_names;

    // So the database can have all of its schemas sent at the top level.
    //
    // It can return a URL or an inline serialization of all saved schemas
    //
    // When the individual schemas are loaded they will be loaded through the
    // cached content that is already present on the disk, or if the schema
    // is serialized inline that will be used.
    //
    if (!populated_entire_set &&
        !collection->source.sha256.empty() &&
        (collection->source.serialized.has_value() || collection->source.url.has_value()))
    {
      auto cache_path = DuckDBHomeDirectory(context);

      // Populate the on-disk schema cache from the catalog while contents_url.
      AirportAPI::PopulateCatalogSchemaCacheFromURLorContent(context, *collection, airport_catalog.internal_name(), cache_path);
    }
    populated_entire_set = true;

    for (const auto &schema : collection->schemas)
    {
      CreateSchemaInfo info;

      if (schema.schema_name().empty())
      {
        throw InvalidInputException("Airport: catalog '%s' contained a schema with an empty name", airport_catalog.internal_name());
      }
      if (!(seen_schema_names.find(schema.schema_name()) == seen_schema_names.end()))
      {
        throw InvalidInputException("Airport: catalog '%s' contained two or more schemas named %s", airport_catalog.internal_name(), schema.schema_name().c_str());
      }

      seen_schema_names.insert(schema.schema_name());

      info.schema = schema.schema_name();
      info.internal = IsInternalTable(schema.catalog_name(), schema.schema_name());
      auto schema_entry = make_uniq<AirportSchemaEntry>(catalog, info, cache_path, schema);

      // Since these are DuckDB attributes, we need to copy them manually.
      schema_entry->comment = schema.comment();
      for (auto &tag : schema.tags())
      {
        schema_entry->tags[tag.first] = tag.second;
      }
      // printf("Creating schema %s\n", schema.schema_name.c_str());
      CreateEntry(std::move(schema_entry));
    }

    called_load_entries = true;
  }

  struct AirportCreateSchemaParameters
  {
    string catalog_name;
    string schema;

    std::optional<string> comment;
    unordered_map<string, string> tags;

    MSGPACK_DEFINE_MAP(catalog_name, schema, comment, tags)
  };

  optional_ptr<CatalogEntry>
  AirportSchemaSet::CreateSchema(ClientContext &context, CreateSchemaInfo &info)
  {
    auto &airport_catalog = catalog.Cast<AirportCatalog>();

    arrow::flight::FlightCallOptions call_options;

    airport_add_standard_headers(call_options, airport_catalog.attach_parameters()->location());
    airport_add_authorization_header(call_options, airport_catalog.attach_parameters()->auth_token());

    call_options.headers.emplace_back("airport-action-name", "create_schema");

    auto flight_client = AirportAPI::FlightClientForLocation(airport_catalog.attach_parameters()->location());

    AirportCreateSchemaParameters params;
    params.catalog_name = airport_catalog.internal_name();
    params.schema = info.schema;
    if (!info.comment.IsNull())
    {
      params.comment = info.comment.ToString();
    }
    // for (auto &tag : info.tags())
    // {
    //   params.tags[tag.first] = tag.second;
    // }

    auto &server_location = airport_catalog.attach_parameters()->location();

    AIRPORT_MSGPACK_ACTION_SINGLE_PARAMETER(action, "create_schema", params);

    auto msgpack_serialized_response = AirportCallAction(flight_client, call_options, action, server_location);

    if (msgpack_serialized_response == nullptr)
    {
      throw AirportFlightException(server_location, "Failed to obtain schema data from Arrow Flight create_schema RPC");
    }

    const auto &body_buffer = msgpack_serialized_response.get()->body;
    AIRPORT_MSGPACK_UNPACK(AirportSerializedContentsWithSHA256Hash, contents,
                           (*body_buffer),
                           server_location,
                           "File to parse msgpack encoded object from create_schema response");

    unordered_map<string, string> empty;
    auto real_entry = AirportAPISchema(
        airport_catalog.internal_name(),
        info.schema,
        "",
        empty,
        contents);

    string cache_path = DuckDBHomeDirectory(context);

    auto schema_entry = make_uniq<AirportSchemaEntry>(catalog, info, cache_path, real_entry);

    return CreateEntry(std::move(schema_entry));
  }

} // namespace duckdb
