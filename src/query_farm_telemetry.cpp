#include "query_farm_telemetry.hpp"
#include "duckdb.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "yyjson.hpp"
#include <cstdlib>
#include <mutex>
#include <thread>

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

namespace {

const char *const TELEMETRY_URL = "https://duckdb-in.query-farm.services/";

// Performs the POST with a client that was created on the loading thread. The
// HTTPParams and the HTTPUtil they reference must outlive the call; the owner
// below guarantees that by joining the worker before the database is torn down
// and, failing that, before the process runs its exit handlers.
void SendTelemetryRequest(HTTPParams &params, unique_ptr<HTTPClient> &client, const string &body) {
	try {
		HTTPHeaders headers;
		headers.Insert("Content-Type", "application/json");
		PostRequestInfo post_request(TELEMETRY_URL, headers, params, const_data_ptr_cast(body.data()), body.size());
		auto response = params.http_util.Request(post_request, client);
	} catch (...) {
		// Telemetry is best-effort: ignore all errors.
	}
}

#ifndef __EMSCRIPTEN__
class QueryFarmTelemetryWorker;

// Process-wide set of workers whose thread may still be running.
//
// The database join below covers every orderly shutdown, but a process can also
// call exit() with the database still alive: the DuckDB shell does so on any
// non-zero exit (a failing statement in `duckdb -f`, `.exit 1`, Ctrl-C x3), and
// embedders may hold a global DuckDB object. exit() then runs the atexit
// handlers -- including OpenSSL's OPENSSL_cleanup, registered by httpfs -- while
// the worker is still inside the request, which is the crash from issue #4.
//
// So the registry also installs an atexit handler that joins every live worker.
// atexit handlers run in reverse registration order, so ours must be registered
// AFTER OpenSSL's to run BEFORE OPENSSL_cleanup. httpfs initialises OpenSSL
// lazily when an HTTP client is first constructed; QueryFarmSendTelemetry
// therefore constructs the client on the loading thread first and only then
// calls EnsureAtExitHandler().
//
// The registry is heap-allocated and never freed on purpose: it has to outlive
// every static destructor in the process.
class QueryFarmTelemetryRegistry {
public:
	static QueryFarmTelemetryRegistry &Get() {
		static QueryFarmTelemetryRegistry *instance = new QueryFarmTelemetryRegistry();
		return *instance;
	}

	void Add(QueryFarmTelemetryWorker &worker) {
		lock_guard<mutex> guard(lock);
		workers.insert(&worker);
	}

	// Once this returns the atexit handler no longer references the worker.
	void Remove(QueryFarmTelemetryWorker &worker) {
		lock_guard<mutex> guard(lock);
		workers.erase(&worker);
	}

	void EnsureAtExitHandler() {
		std::call_once(atexit_once, []() { std::atexit(&QueryFarmTelemetryRegistry::JoinAllAtExit); });
	}

private:
	QueryFarmTelemetryRegistry() = default;
	static void JoinAllAtExit();

	mutex lock;
	unordered_set<QueryFarmTelemetryWorker *> workers;
	std::once_flag atexit_once;
};

// Owns the background thread that sends the telemetry ping.
//
// A plain detached thread could outlive the DuckDB instance that created it and
// race the process teardown (issue #4). Storing the worker in the database's
// ObjectCache ties it to the database instead: ~DatabaseInstance destroys the
// cache before the scheduler, buffer manager, and log manager, so the join
// happens while everything the request touches is still intact. The registry
// above covers the remaining case where the process exits without ever
// destroying the database.
class QueryFarmTelemetryWorker : public ObjectCacheEntry {
public:
	QueryFarmTelemetryWorker(unique_ptr<HTTPParams> params, unique_ptr<HTTPClient> client, string body)
	    : params(std::move(params)), client(std::move(client)), body(std::move(body)) {
	}

	~QueryFarmTelemetryWorker() override {
		// Unregister first: after this the atexit handler cannot touch us, so the
		// join below cannot race it.
		QueryFarmTelemetryRegistry::Get().Remove(*this);
		Join();
	}

	void Start() {
		QueryFarmTelemetryRegistry::Get().Add(*this);
		try {
			worker = std::thread([this]() { SendTelemetryRequest(*params, client, body); });
		} catch (...) {
			// Could not spawn a thread; drop the ping.
		}
	}

	void Join() {
		if (worker.joinable()) {
			worker.join();
		}
	}

	static string ObjectType() {
		return "query_farm_telemetry_worker";
	}

	string GetObjectType() override {
		return ObjectType();
	}

	// Not evictable: an eviction would destroy this entry (and join the thread)
	// at an arbitrary moment. The entry must live exactly as long as the database.
	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx();
	}

private:
	unique_ptr<HTTPParams> params;
	unique_ptr<HTTPClient> client;
	string body;
	std::thread worker;
};

void QueryFarmTelemetryRegistry::JoinAllAtExit() {
	auto &registry = Get();
	lock_guard<mutex> guard(registry.lock);
	for (auto *worker : registry.workers) {
		worker->Join();
	}
}
#endif

string BuildTelemetryBody(const string &extension_name, const string &extension_version) {
	auto doc = yyjson_mut_doc_new(nullptr);
	auto result_obj = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, result_obj);

	auto platform = DuckDB::Platform();

	yyjson_mut_obj_add_str(doc, result_obj, "extension_name", extension_name.c_str());
	yyjson_mut_obj_add_str(doc, result_obj, "extension_version", extension_version.c_str());
	yyjson_mut_obj_add_str(doc, result_obj, "user_agent", "query-farm/20260201");
	yyjson_mut_obj_add_str(doc, result_obj, "duckdb_platform", platform.c_str());
	yyjson_mut_obj_add_str(doc, result_obj, "duckdb_library_version", DuckDB::LibraryVersion());
	yyjson_mut_obj_add_str(doc, result_obj, "duckdb_release_codename", DuckDB::ReleaseCodename());
	yyjson_mut_obj_add_str(doc, result_obj, "duckdb_source_id", DuckDB::SourceID());

	size_t telemetry_len = 0;
	auto telemetry_data =
	    yyjson_mut_val_write_opts(result_obj, YYJSON_WRITE_ALLOW_INF_AND_NAN, NULL, &telemetry_len, nullptr);
	yyjson_mut_doc_free(doc);

	if (telemetry_data == nullptr) {
		throw SerializationException("Failed to serialize telemetry data.");
	}
	string body(telemetry_data, telemetry_len);
	free(telemetry_data);
	return body;
}

} // namespace

INTERNAL_FUNC void QueryFarmSendTelemetry(ExtensionLoader &loader, const string &extension_name,
                                          const string &extension_version) {
	const char *opt_out = std::getenv("QUERY_FARM_TELEMETRY_OPT_OUT");
	if (opt_out != nullptr) {
		return;
	}

	auto &db = loader.GetDatabaseInstance();
	try {
		ExtensionHelper::TryAutoLoadExtension(db, "httpfs");
	} catch (...) {
		return;
	}
	if (!db.ExtensionIsLoaded("httpfs")) {
		return;
	}

	// Everything from here on is best-effort. Building the payload and resolving
	// HTTP parameters can throw (for example, HTTPParams::Initialize rejects an
	// http_proxy value it cannot parse); none of that may fail the extension load.
	try {
		auto body = BuildTelemetryBody(extension_name, extension_version);

		// Resolve settings, proxy, and secrets on the loading thread, so the worker
		// touches nothing but the HTTP client. Bound the request so a stalled
		// connection cannot hold up database shutdown (or process exit) for long,
		// and never retry -- a dropped ping is not worth one. Note that the httplib
		// client applies this separately to connect, write and read, so the real
		// bound is a small multiple of it; curl applies it as a total.
		auto &http_util = HTTPUtil::Get(db);
		unique_ptr<HTTPParams> params = http_util.InitializeParameters(db, TELEMETRY_URL);
		params->timeout = 3;
		params->timeout_usec = 0;
		params->retries = 0;

		// Construct the client on the loading thread as well. Besides keeping the
		// worker minimal, this is what initialises curl/OpenSSL (and registers
		// OpenSSL's atexit cleanup) before we register our own exit handler below,
		// which is what guarantees ours runs first. No network I/O happens here.
		string path, proto_host_port;
		HTTPUtil::DecomposeURL(TELEMETRY_URL, path, proto_host_port);
		auto client = http_util.InitializeClient(*params, proto_host_port);
		if (!client) {
			return;
		}

#ifndef __EMSCRIPTEN__
		QueryFarmTelemetryRegistry::Get().EnsureAtExitHandler();

		auto worker = make_shared_ptr<QueryFarmTelemetryWorker>(std::move(params), std::move(client), std::move(body));
		// Register with the database before starting so it owns the thread from
		// the moment it exists.
		db.GetObjectCache().Put("query_farm_telemetry-" + extension_name, worker);
		worker->Start();
#else
		SendTelemetryRequest(*params, client, body);
#endif
	} catch (...) {
		// Telemetry must never affect extension load.
	}
}

} // namespace duckdb
