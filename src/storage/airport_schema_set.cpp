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

    const string home_directory = fs.GetHomeDirectory();
    // exception if the home directory does not exist, don't create whatever we think is home
    if (!fs.DirectoryExists(home_directory))
    {
      throw IOException("Can't find the home directory at '%s'\nSpecify a home directory using the SET "
                        "home_directory='/path/to/dir' option.",
                        home_directory);
    }
    return fs.JoinPath(home_directory, ".duckdb");
  }

  void AirportSchemaSet::LoadEntireSet(DatabaseInstance &db)
  {
    lock_guard<mutex> l(entry_lock);

    if (called_load_entries == false)
    {
      // We haven't called load entries yet.
      LoadEntries(db);
      called_load_entries = true;
    }
  }

  static string AirportDuckDBHomeDirectory(DatabaseInstance &db)
  {
    auto &fs = db.GetFileSystem();

    const string home_directory = fs.GetHomeDirectory();
    // exception if the home directory does not exist, don't create whatever we think is home
    if (!fs.DirectoryExists(home_directory))
    {
      throw IOException("Can't find the home directory at '%s'\nSpecify a home directory using the SET "
                        "home_directory='/path/to/dir' option.",
                        home_directory);
    }
    return fs.JoinPath(home_directory, ".duckdb");
  }

  void AirportSchemaSet::LoadEntries(DatabaseInstance &db)
  {
    if (called_load_entries)
    {
      return;
    }

    auto &airport_catalog = catalog.Cast<AirportCatalog>();
    const string cache_path = AirportDuckDBHomeDirectory(db);
    const auto &catalog_name = airport_catalog.internal_name();

    auto returned_collection = AirportAPI::GetSchemas(
        catalog_name, airport_catalog.attach_parameters());

    airport_catalog.SetLoadedCatalogVersion(returned_collection->version_info);
    collection = std::move(returned_collection);

    if (!populated_entire_set &&
        !collection->source.sha256.empty() &&
        (collection->source.serialized.has_value() ||
         collection->source.url.has_value()))
    {
      AirportAPI::PopulateCatalogSchemaCacheFromURLorContent(
          db, *collection, catalog_name, cache_path);
    }

    populated_entire_set = true;

    std::unordered_set<string> seen_schema_names;

    for (const auto &schema : collection->schemas)
    {
      const auto &schema_name = schema.schema_name();

      if (schema_name.empty())
      {
        throw InvalidInputException("Airport: catalog '%s' contains a schema with an empty name", catalog_name);
      }
      if (!seen_schema_names.insert(schema_name).second)
      {
        throw InvalidInputException("Airport: catalog '%s' contains duplicate schema name '%s'", catalog_name, schema_name.c_str());
      }

      CreateSchemaInfo info;
      info.schema = schema_name;
      info.internal = IsInternalTable(schema.catalog_name(), schema_name);

      auto schema_entry = make_uniq<AirportSchemaEntry>(catalog, info, cache_path, schema);
      schema_entry->comment = schema.comment();

      for (const auto &[key, value] : schema.tags())
      {
        schema_entry->tags[key] = value;
      }

      if (schema.is_default() && default_schema_.empty())
      {
        default_schema_ = schema_name;
      }

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
    AirportSerializedContentsWithSHA256Hash contents;
    AIRPORT_MSGPACK_UNPACK(contents,
                           (*body_buffer),
                           server_location,
                           "File to parse msgpack encoded object from create_schema response");

    unordered_map<string, string> empty;
    auto real_entry = AirportAPISchema(
        airport_catalog.internal_name(),
        info.schema,
        "",
        empty,
        false,
        contents);

    string cache_path = DuckDBHomeDirectory(context);

    auto schema_entry = make_uniq<AirportSchemaEntry>(catalog, info, cache_path, real_entry);

    return CreateEntry(std::move(schema_entry));
  }

  std::string AirportSchemaSet::GetDefaultSchema(DatabaseInstance &db)
  {
    LoadEntireSet(db);

    return default_schema_;
  }

} // namespace duckdb
