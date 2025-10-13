#include "query_farm_telemetry.hpp"
#include <thread>
#include "duckdb.hpp"
#include "duckdb/common/http_util.hpp"
#include "yyjson.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/config.hpp"
#include <cstdlib>
#include <future>
using namespace duckdb_yyjson; // NOLINT

namespace duckdb
{

	namespace
	{

		// Function to send the actual HTTP request
		void sendHTTPRequest(shared_ptr<DatabaseInstance> db, char *json_body, size_t json_body_size)
		{
			const string TARGET_URL("https://duckdb-in.query-farm.services/");

			HTTPHeaders headers;
			headers.Insert("Content-Type", "application/json");

			auto &http_util = HTTPUtil::Get(*db);
			unique_ptr<HTTPParams> params = http_util.InitializeParameters(*db, TARGET_URL);

			PostRequestInfo post_request(TARGET_URL, headers, *params, reinterpret_cast<const_data_ptr_t>(json_body),
																	 json_body_size);
			try
			{
				auto response = http_util.Request(post_request);
			}
			catch (const std::exception &e)
			{
				// ignore all errors.
			}

			free(json_body);
			return;
		}

	} // namespace

	INTERNAL_FUNC void QueryFarmSendTelemetry(ExtensionLoader &loader, const string &extension_name,
																						const string &extension_version)
	{
		const char *opt_out = std::getenv("QUERY_FARM_TELEMETRY_OPT_OUT");
		if (opt_out != nullptr)
		{
			return;
		}

		auto &dbconfig = DBConfig::GetConfig(loader.GetDatabaseInstance());
		auto old_value = dbconfig.options.autoinstall_known_extensions;
		dbconfig.options.autoinstall_known_extensions = false;
		try
		{
			ExtensionHelper::AutoLoadExtension(loader.GetDatabaseInstance(), "httpfs");
		}
		catch (...)
		{
			dbconfig.options.autoinstall_known_extensions = old_value;
			return;
		}

		dbconfig.options.autoinstall_known_extensions = old_value;
		if (!loader.GetDatabaseInstance().ExtensionIsLoaded("httpfs"))
		{
			return;
		}

		// Initialize the telemetry sender
		auto doc = yyjson_mut_doc_new(nullptr);

		auto result_obj = yyjson_mut_obj(doc);
		yyjson_mut_doc_set_root(doc, result_obj);

		auto platform = DuckDB::Platform();

		yyjson_mut_obj_add_str(doc, result_obj, "extension_name", extension_name.c_str());
		yyjson_mut_obj_add_str(doc, result_obj, "extension_version", extension_version.c_str());
		yyjson_mut_obj_add_str(doc, result_obj, "user_agent", "query-farm/20251011");
		yyjson_mut_obj_add_str(doc, result_obj, "duckdb_platform", platform.c_str());
		yyjson_mut_obj_add_str(doc, result_obj, "duckdb_library_version", DuckDB::LibraryVersion());
		yyjson_mut_obj_add_str(doc, result_obj, "duckdb_release_codename", DuckDB::ReleaseCodename());
		yyjson_mut_obj_add_str(doc, result_obj, "duckdb_source_id", DuckDB::SourceID());

		size_t telemetry_len;
		auto telemetry_data =
				yyjson_mut_val_write_opts(result_obj, YYJSON_WRITE_ALLOW_INF_AND_NAN, NULL, &telemetry_len, nullptr);

		if (telemetry_data == nullptr)
		{
			throw SerializationException("Failed to serialize telemetry data.");
		}

		yyjson_mut_doc_free(doc);

#ifndef __EMSCRIPTEN__
		[[maybe_unused]] auto _ = std::async(
				std::launch::async, [db_ptr = loader.GetDatabaseInstance().shared_from_this(), json = telemetry_data,
														 len = telemetry_len]() mutable
				{ sendHTTPRequest(std::move(db_ptr), json, len); });
#else
		sendHTTPRequest(loader.GetDatabaseInstance().shared_from_this(), telemetry_data, telemetry_len);
#endif
	}

} // namespace duckdb