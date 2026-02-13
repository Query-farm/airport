#include "airport_extension.hpp"
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include "storage/airport_catalog_api.hpp"
#include "storage/airport_catalog.hpp"

#include "duckdb/common/file_system.hpp"

#include "airport_macros.hpp"
#include "airport_request_headers.hpp"
#include "duckdb/common/arrow/schema_metadata.hpp"
#include "airport_schema_utils.hpp"
#include "airport_rpc.hpp"
#include "duckdb/common/http_util.hpp"
#include "mbedtls_wrapper.hpp"

namespace flight = arrow::flight;

namespace duckdb
{

  static constexpr idx_t FILE_FLAGS_READ = idx_t(1 << 0);
  static constexpr idx_t FILE_FLAGS_WRITE = idx_t(1 << 1);
  //  static constexpr idx_t FILE_FLAGS_FILE_CREATE = idx_t(1 << 3);
  static constexpr idx_t FILE_FLAGS_FILE_CREATE_NEW = idx_t(1 << 4);

  namespace
  {
    void writeToTempFile(FileSystem &fs, const string &tempFilename, const std::string_view &data)
    {
      auto handle = fs.OpenFile(tempFilename, FILE_FLAGS_WRITE | FILE_FLAGS_FILE_CREATE_NEW);
      if (!handle)
      {
        throw IOException("Airport: Failed to open file for writing: %s", tempFilename.c_str());
      }

      handle->Write((void *)data.data(), data.size());
      handle->Sync();
      handle->Close();
    }

    string decompressZStandard(const string &source, const int decompressed_size, const string &location)
    {
      AIRPORT_ASSIGN_OR_RAISE_LOCATION(
          auto codec,
          arrow::util::Codec::Create(arrow::Compression::ZSTD),
          location, "");

      AIRPORT_ASSIGN_OR_RAISE_LOCATION(auto decompressed_data,
                                       ::arrow::AllocateBuffer(decompressed_size),
                                       location,
                                       "");

      auto decompress_result = codec->Decompress(
          source.size(),
          reinterpret_cast<const uint8_t *>(source.data()),
          decompressed_size,
          decompressed_data->mutable_data());

      AIRPORT_ARROW_ASSERT_OK_LOCATION(decompress_result, location, "Failed to decompress the schema data");

      return string(reinterpret_cast<const char *>(decompressed_data->data()), decompressed_size);
    }

    string generateTempFilename(FileSystem &fs, const string &dir)
    {
      static std::random_device rd;
      static std::mt19937 gen(rd());
      static std::uniform_int_distribution<> dis(0, 999999);

      string filename;

      do
      {
        filename = fs.JoinPath(dir, "temp_" + std::to_string(dis(gen)) + ".tmp");
      } while (fs.FileExists(filename));
      return filename;
    }

    std::string readFromFile(FileSystem &fs, const string &filename)
    {
      auto handle = fs.OpenFile(filename, FILE_FLAGS_READ);
      if (!handle)
      {
        return "";
      }
      auto file_size = handle->GetFileSize();
      string read_buffer = string(file_size, '\0');
      size_t bytes_read = 0;
      while (bytes_read != file_size)
      {
        auto read_bytes = handle->Read((char *)read_buffer.data() + bytes_read, file_size - bytes_read);
        bytes_read += read_bytes;
      }
      return read_buffer;
    }
  }

  vector<string> AirportAPI::GetCatalogs(const string &catalog, AirportAttachParameters credentials)
  {
    throw NotImplementedException("AirportAPI::GetCatalogs");
  }

  static std::unordered_map<std::string, std::shared_ptr<flight::FlightClient>> airport_flight_clients_by_location;
  static std::mutex airport_clients_mutex;

  std::shared_ptr<flight::FlightClient> AirportAPI::FlightClientForLocation(const std::string &location)
  {
    std::lock_guard<std::mutex> lock(airport_clients_mutex);
    auto it = airport_flight_clients_by_location.find(location);
    if (it != airport_flight_clients_by_location.end())
    {
      return it->second;
    }

    AIRPORT_ASSIGN_OR_RAISE_LOCATION(auto parsed_location,
                                     flight::Location::Parse(location), location, "");
    AIRPORT_ASSIGN_OR_RAISE_LOCATION(airport_flight_clients_by_location[location],
                                     flight::FlightClient::Connect(parsed_location),
                                     location, "");

    return airport_flight_clients_by_location[location];
  }

  namespace
  {
    std::string SHA256ForString(const std::string_view &input)
    {
      char result[duckdb_mbedtls::MbedTlsWrapper::SHA256_HASH_LENGTH_TEXT + 1];
      result[duckdb_mbedtls::MbedTlsWrapper::SHA256_HASH_LENGTH_TEXT] = '\0';

      duckdb_mbedtls::MbedTlsWrapper::SHA256State state;
      state.AddBytes((duckdb::data_ptr_t)input.data(), input.size());
      state.FinishHex(result);
      return result;
    }

    std::string SHA256ForString(const std::string &input)
    {
      char result[duckdb_mbedtls::MbedTlsWrapper::SHA256_HASH_LENGTH_TEXT + 1];
      result[duckdb_mbedtls::MbedTlsWrapper::SHA256_HASH_LENGTH_TEXT] = '\0';

      duckdb_mbedtls::MbedTlsWrapper::SHA256State state;
      state.AddString(input);
      state.FinishHex(result);
      return result;
    }

    std::pair<HTTPStatusCode, std::string> GetRequest(DatabaseInstance &db, const string &url, const string expected_sha256)
    {
      HTTPHeaders headers;

      auto &http_util = HTTPUtil::Get(db);
      unique_ptr<HTTPParams> params;
      auto target_url = string(url);
      params = http_util.InitializeParameters(db, target_url);

      GetRequestInfo get_request(target_url,
                                 headers,
                                 *params,
                                 nullptr,
                                 nullptr);

      auto response = http_util.Request(get_request);

      if (response->status != HTTPStatusCode::OK_200 || expected_sha256.empty())
      {
        return std::make_pair(response->status, "");
      }

      // Verify that the SHA256 matches the returned data, don't want a server to
      // corrupt the data.
      auto buffer_view = std::string_view(response->body.data(), response->body.size());
      auto encountered_sha256 = SHA256ForString(buffer_view);

      if (encountered_sha256 != expected_sha256)
      {
        throw IOException("Airport: SHA256 mismatch for URL: " + url);
      }
      return std::make_pair(response->status, response->body);
    }

#undef CreateDirectory
    std::pair<const string, const string> GetCachePath(FileSystem &fs, const string &input, const string &baseDir)
    {
      auto cacheDir = fs.JoinPath(baseDir, "airport_cache");
      if (!fs.DirectoryExists(baseDir))
      {
        fs.CreateDirectory(baseDir);
      }

      if (!fs.DirectoryExists(cacheDir))
      {
        fs.CreateDirectory(cacheDir);
      }

      if (input.size() < 6)
      {
        throw std::invalid_argument("String is too short to contain the SHA256");
      }

      auto subDirName = input.substr(0, 3); // First 3 characters for subdirectory
      auto fileName = input.substr(3);      // Remaining characters for filename

      auto subDir = fs.JoinPath(cacheDir, subDirName);
      if (!fs.DirectoryExists(subDir))
      {
        fs.CreateDirectory(subDir);
      }

      return std::make_pair(subDir, fs.JoinPath(subDir, fileName));
    }
  }

#undef MoveFile
  void AirportAPI::PopulateCatalogSchemaCacheFromURLorContent(DatabaseInstance &db,
                                                              const AirportSchemaCollection &collection,
                                                              const string &catalog_name,
                                                              const string &baseDir)
  {
    auto fs = FileSystem::CreateLocal();

    if (collection.source.sha256.empty())
    {
      // If the collection doesn't have a SHA256 there isn't anything we can do.
      throw IOException("Catalog " + catalog_name + " has no SHA256 value for its contents");
    }

    // If there is a large amount of data to be populate its useful to determine
    // if the SHA256 value of all schemas has already been populated.
    //
    // So we have a sentinel path that indicates the entire contents of the SHA256
    // has already been written out to the cache.
    auto sentinel_paths = GetCachePath(*fs, collection.source.sha256, baseDir);

    if (fs->FileExists(sentinel_paths.second))
    {
      // All of the schemas for this overall SHA256
      // have been populated so there is nothing to do.
      return;
    }

    // The schema data is serialized using msgpack
    //
    // it can either be retrieved from a URL or provided inline.
    //
    // The schemas are packed like:
    // [
    //   [string, string]
    // ]
    // the first item is the SHA256 of the data, then the actual data
    // is the second item.
    //
    msgpack::object_handle oh;
    if (collection.source.serialized.has_value())
    {
      const string found_sha = SHA256ForString(collection.source.serialized.value());
      if (found_sha != collection.source.sha256)
      {
        throw IOException("Catalog " + catalog_name + " SHA256 Mismatch expected " + collection.source.sha256 + " found " + found_sha);
      }
      auto &data = collection.source.serialized.value();
      oh = msgpack::unpack(reinterpret_cast<const char *>(data.data()), data.size(), 0);
    }
    else if (collection.source.url.has_value())
    {
      // How do we know if the URLs haven't already been populated.
      auto get_result = GetRequest(db, collection.source.url.value(), collection.source.sha256);

      if (get_result.first != HTTPStatusCode::OK_200)
      {
        throw IOException("Catalog " + catalog_name + " failed to retrieve schema collection contents from URL: " + collection.source.url.value());
      }
      oh = msgpack::unpack(reinterpret_cast<const char *>(get_result.second.data()), get_result.second.size(), 0);
    }
    else
    {
      throw IOException("Catalog " + catalog_name + " has no serialized or URL contents for its schema collection");
    }

    std::vector<std::vector<std::string>> unpacked_data = oh.get().as<std::vector<std::vector<std::string>>>();

    // Each item contained in the serailized catalog will be a SHA256 then the
    // the actual value.
    //
    // the SHA256 will be used to check, if the data is corrupted.
    // then used as part of the filename to store the data on disk.
    for (auto &item : unpacked_data)
    {
      if (item.size() != 2)
      {
        throw IOException("Catalog schema cache contents had an item where size != 2");
      }

      const string &expected_sha = item[0];
      const string found_sha = SHA256ForString(item[1]);
      if (found_sha != expected_sha)
      {
        auto error_prefix = "Catalog " + catalog_name + " SHA256 Mismatch expected " + expected_sha + " found " + found_sha;

        // There is corruption.
        if (collection.source.url.has_value())
        {
          throw IOException(error_prefix + " from URL: " + collection.source.url.value());
        }
        else
        {
          throw IOException(error_prefix + " from serialized content");
        }
      }

      auto paths = GetCachePath(*fs, item[0], baseDir);
      auto tempFilename = generateTempFilename(*fs, paths.first);

      writeToTempFile(*fs, tempFilename, item[1]);

      // Rename the temporary file to the final filename
      if (!fs->FileExists(paths.second))
      {
        fs->MoveFile(tempFilename, paths.second);
      }
    }

    // There is a bit of a problem here, we could have an infinite set of
    // schema temp files being written, because if a server is dynamically generating
    // its schemas, the SHA256 will always change.
    //
    // Rusty address this later on.

    // Write a file that the cache has been populated with the top level SHA256
    // value, so that we can skip doing this next time all schemas are used.
    writeToTempFile(*fs, sentinel_paths.second, "1");
  }

  namespace
  {
    // Function to handle caching
    std::pair<HTTPStatusCode, std::string> getCachedRequestData(DatabaseInstance &db,
                                                                const AirportSerializedContentsWithSHA256Hash &source,
                                                                const string &baseDir)
    {
      if (source.sha256.empty())
      {
        // Can't cache anything since we don't know the expected sha256 value.
        // and the caching is based on the sha256 values.
        //
        // So if there was inline content supplied use that and fake that it was
        // retrieved from a server.
        if (source.serialized.has_value())
        {
          return std::make_pair(HTTPStatusCode::OK_200, source.serialized.value());
        }
        else if (source.url.has_value())
        {
          return GetRequest(db, source.url.value(), source.sha256);
        }
        else
        {
          throw IOException("SHA256 is empty and URL is empty");
        }
      }

      // If the user supplied an inline serialized value, check if the sha256 matches, if so
      // use it otherwise fall abck to the url
      if (source.serialized.has_value())
      {
        if (SHA256ForString(source.serialized.value()) == source.sha256)
        {
          return std::make_pair(HTTPStatusCode::OK_200, source.serialized.value());
        }
        if (!source.url.has_value())
        {
          throw IOException("SHA256 mismatch for inline serialized data and URL is empty");
        }
      }

      auto fs = FileSystem::CreateLocal();

      auto paths = GetCachePath(*fs, source.sha256, baseDir);

      // Check if data is in cache
      if (fs->FileExists(paths.second))
      {
        std::string cachedData = readFromFile(*fs, paths.second);
        if (!cachedData.empty())
        {
          // Verify that the SHA256 matches the returned data, don't allow a corrupted filesystem
          // to affect things.
          if (!source.sha256.empty() && SHA256ForString(cachedData) != source.sha256)
          {
            throw IOException("SHA256 mismatch for URL: %s from cached data at %s, check for cache corruption", source.url.value(), paths.second.c_str());
          }
          return std::make_pair(HTTPStatusCode::OK_200, cachedData);
        }
      }

      // I know this doesn't work for zero byte cached responses, its okay.

      // Data not in cache, fetch it
      auto get_result = GetRequest(db, source.url.value(), source.sha256);

      if (get_result.first != HTTPStatusCode::OK_200)
      {
        return get_result;
      }

      // Save the fetched data to a temporary file
      auto tempFilename = generateTempFilename(*fs, paths.first);
      auto content = std::string_view(get_result.second.data(), get_result.second.size());
      writeToTempFile(*fs, tempFilename, content);

      // Rename the temporary file to the final filename
      if (!fs->FileExists(paths.second))
      {
        fs->MoveFile(tempFilename, paths.second);
      }
      return get_result;
    }

    const AirportSerializedFlightAppMetadata ParseFlightAppMetadata(const string &app_metadata, const string &server_location)
    {
      AirportSerializedFlightAppMetadata app_metadata_obj;
      AIRPORT_MSGPACK_UNPACK(app_metadata_obj,
                             app_metadata,
                             server_location,
                             "Failed to parse Flight app_metadata");
      return app_metadata_obj;
    }

    static void handle_flight_app_metadata(const string &app_metadata,
                                           const string &catalog_name,
                                           const string &schema_name,
                                           const string &server_location,
                                           const flight::FlightDescriptor &descriptor,
                                           std::shared_ptr<arrow::Schema> schema,
                                           const unique_ptr<AirportSchemaContents> &contents)
    {
      auto parsed_app_metadata = ParseFlightAppMetadata(app_metadata, server_location);
      if (parsed_app_metadata.catalog != catalog_name)
      {
        throw AirportFlightException(server_location, descriptor, string("Flight metadata catalog name does not match expected value expected '" + catalog_name + "' found '" + parsed_app_metadata.catalog + "'"), "");
      }
      if (parsed_app_metadata.schema != schema_name)
      {
        throw AirportFlightException(server_location, descriptor, string("Flight metadata schema name does not match expected value, expected '" + schema_name + "' found '" + parsed_app_metadata.schema + "'"), "");
      }

      const auto &type = parsed_app_metadata.type;

      if (type == "table")
      {
        contents->tables.emplace_back(AirportAPITable(
            server_location,
            descriptor,
            schema,
            parsed_app_metadata));
      }
      else if (type == "table_function")
      {
        contents->table_functions.emplace_back(AirportAPITableFunction(
            server_location,
            descriptor,
            schema,
            parsed_app_metadata));
      }
      else if (type == "scalar_function")
      {
        contents->scalar_functions.emplace_back(AirportAPIScalarFunction(
            server_location,
            descriptor,
            schema,
            parsed_app_metadata));
      }
      else
      {
        throw AirportFlightException(server_location, descriptor, "Unknown object type in app_metadata", type);
      }
    }
  }

  unique_ptr<AirportSchemaContents>
  AirportAPI::GetSchemaItems(DatabaseInstance &db,
                             const string &catalog,
                             const string &schema,
                             const AirportSerializedContentsWithSHA256Hash &source,
                             const string &cache_base_dir,
                             std::shared_ptr<AirportAttachParameters> credentials)
  {

    auto contents = make_uniq<AirportSchemaContents>();

    const auto &server_location = credentials->location();

    // So the value can be provided by an external URL or inline, but either way
    // we must have a SHA256 provided, because if the overall catalog populated the
    // cache at the top level it wrote the data for each schema to a sha256 named file
    // in the on disk cache.
    //
    // this is still a bit messy.
    if (
        source.url.has_value() ||
        source.serialized.has_value() ||
        !source.sha256.empty())
    {
      string url_contents;
      auto get_response = getCachedRequestData(db, source, cache_base_dir);

      if (get_response.first != HTTPStatusCode::OK_200)
      {
        throw IOException("Failed to get Airport schema contents from URL: %s http response code %ld", source.url.value(), get_response.first);
      }
      url_contents = get_response.second;

      // So this data has this layout
      AirportSerializedCompressedContent compressed_content;
      AIRPORT_MSGPACK_UNPACK(compressed_content, url_contents, server_location, "File to parse msgpack encoded object AirportSerializedCompressedContent");

      auto decompressed_url_contents = decompressZStandard(compressed_content.data, compressed_content.length, server_location);

      std::vector<std::string> unpacked_data;
      try
      {
        msgpack::object_handle oh = msgpack::unpack(
            reinterpret_cast<const char *>(decompressed_url_contents.data()),
            compressed_content.length,
            0);
        unpacked_data = oh.get().as<std::vector<std::string>>();
      }
      catch (const std::exception &e)
      {
        throw AirportFlightException(server_location, "Failed to parse msgpack encoded schema items: " + string(e.what()));
      }

      for (auto item : unpacked_data)
      {
        AIRPORT_ASSIGN_OR_RAISE_LOCATION(auto flight_info, arrow::flight::FlightInfo::Deserialize(item), server_location, "");

        // Look in api_metadata for each flight and determine if it should be handled
        // if there is no metadata specified on a flight it is ignored.
        auto app_metadata = flight_info->app_metadata();
        if (!app_metadata.empty())
        {
          auto output_schema = AirportAPIObjectBase::GetSchema(server_location, *flight_info);

          handle_flight_app_metadata(app_metadata, catalog, schema, server_location,
                                     flight_info->descriptor(),
                                     output_schema, contents);
        }
      }

      return contents;
    }
    else
    {
      // We need to load the contents of the schemas by listing the flights.
      arrow::flight::FlightCallOptions call_options;
      airport_add_standard_headers(call_options, credentials->location());
      airport_add_catalog_header(call_options, catalog);
      call_options.headers.emplace_back("airport-list-flights-filter-catalog", catalog);
      call_options.headers.emplace_back("airport-list-flights-filter-schema", schema);

      airport_add_authorization_header(call_options, credentials->auth_token());

      auto flight_client = FlightClientForLocation(credentials->location());

      AIRPORT_ASSIGN_OR_RAISE_LOCATION(auto listing, flight_client->ListFlights(call_options, {credentials->criteria()}), server_location, "");

      std::shared_ptr<flight::FlightInfo> flight_info;
      AIRPORT_ASSIGN_OR_RAISE_LOCATION(flight_info, listing->Next(), server_location, "");

      while (flight_info != nullptr)
      {
        // Look in api_metadata for each flight and determine if it should be a table.
        auto app_metadata = flight_info->app_metadata();
        if (!app_metadata.empty())
        {
          auto output_schema = AirportAPIObjectBase::GetSchema(server_location, *flight_info);
          handle_flight_app_metadata(app_metadata, catalog, schema, server_location, flight_info->descriptor(), output_schema, contents);
        }
        AIRPORT_ASSIGN_OR_RAISE_LOCATION(flight_info, listing->Next(), server_location, "");
      }

      return contents;
    }
  }

  LogicalType
  AirportAPI::GetRowIdType(ClientContext &context,
                           const std::shared_ptr<arrow::Schema> &schema,
                           const AirportLocationDescriptor &location_descriptor)
  {
    ArrowSchemaWrapper schema_root;
    AIRPORT_ARROW_ASSERT_OK_CONTAINER(ExportSchema(*schema,
                                                   &schema_root.arrow_schema),
                                      &location_descriptor,
                                      "ExportSchema");
    const auto number_of_columns = (idx_t)schema_root.arrow_schema.n_children;

    for (idx_t col_idx = 0;
         col_idx < number_of_columns; col_idx++)
    {
      auto &schema_child = *schema_root.arrow_schema.children[col_idx];
      if (!schema_child.release)
      {
        throw InvalidInputException("airport_take_flight: released schema passed");
      }

      if (AirportFieldMetadataIsRowId(schema_child.metadata))
      {
        auto arrow_type = AirportGetArrowType(context, schema_child);
        return arrow_type->GetDuckType();
      }
    }
    return LogicalType::SQLNULL;
  }

  unique_ptr<AirportSchemaCollection>
  AirportAPI::GetSchemas(const string &catalog_name,
                         const std::shared_ptr<AirportAttachParameters> &credentials)
  {
    auto result = make_uniq<AirportSchemaCollection>();
    arrow::flight::FlightCallOptions call_options;
    airport_add_standard_headers(call_options, credentials->location());
    airport_add_catalog_header(call_options, catalog_name);
    airport_add_authorization_header(call_options, credentials->auth_token());

    auto flight_client = FlightClientForLocation(credentials->location());

    AirportSerializedCatalogSchemaRequest catalog_request = {catalog_name};

    AIRPORT_MSGPACK_ACTION_SINGLE_PARAMETER(action, "list_schemas", catalog_request);

    std::unique_ptr<arrow::flight::ResultStream> action_results;

    auto &server_location = credentials->location();

    auto msgpack_serialized_response = AirportCallAction(
        flight_client,
        call_options,
        action,
        server_location);

    if (msgpack_serialized_response == nullptr)
    {
      throw AirportFlightException(server_location, "Failed to obtain schema data from Arrow Flight server via DoAction()");
    }

    const auto &body_buffer = msgpack_serialized_response.get()->body;
    AirportSerializedCompressedContent compressed_content;
    AIRPORT_MSGPACK_UNPACK(compressed_content,
                           (*body_buffer),
                           server_location,
                           "File to parse msgpack encoded object describing Arrow Flight schema data");

    auto decompressed_schema_data = decompressZStandard(compressed_content.data, compressed_content.length, server_location);

    AirportSerializedCatalogRoot catalog_root;
    AIRPORT_MSGPACK_UNPACK(catalog_root,
                           decompressed_schema_data,
                           server_location,
                           "Failed to unpack msgpack AirportSerializedCatalogRoot.");

    result->source = catalog_root.contents;
    result->version_info = catalog_root.version_info;

    for (auto &schema : catalog_root.schemas)
    {
      result->schemas.push_back(AirportAPISchema(
          catalog_name,
          schema.name,
          schema.description,
          schema.tags,
          schema.is_default.value_or(false),
          schema.contents));
    }

    return result;
  }

} // namespace duckdb
