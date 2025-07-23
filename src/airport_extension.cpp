#include "airport_extension.hpp"
#include "duckdb.hpp"

#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/function/table_macro_function.hpp"
#include "duckdb/catalog/default/default_functions.hpp"
#include "storage/airport_catalog.hpp"
#include "storage/airport_transaction_manager.hpp"
#include "airport_secrets.hpp"
#include "airport_optimizer.hpp"
#include "airport_scalar_function.hpp"
#include "airport_json_common.hpp"
#include "airport_telemetry.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "airport_logging.hpp"

#define AIRPORT_EXTENSION_VERSION "20250723.01"

namespace duckdb
{

    static unique_ptr<BaseSecret> CreateAirportSecretFunction(ClientContext &, CreateSecretInput &input)
    {
        // apply any overridden settings
        vector<string> prefix_paths;

        auto scope = input.scope;
        if (scope.empty())
        {
            throw InternalException("No scope set Airport create secret (should start with grpc://): '%s'", input.type);
        }

        auto result = make_uniq<KeyValueSecret>(scope, "airport", "config", input.name);
        for (const auto &named_param : input.options)
        {
            auto lower_name = StringUtil::Lower(named_param.first);

            if (lower_name == "auth_token")
            {
                result->secret_map["auth_token"] = named_param.second.ToString();
            }
            else
            {
                throw InternalException("Unknown named parameter passed to CreateAirportSecretFunction: " + lower_name);
            }
        }

        //! Set redact keys
        result->redact_keys = {"auth_token"};

        return result;
    }

    static void AirportSetSecretParameters(CreateSecretFunction &function)
    {
        function.named_parameters["auth_token"] = LogicalType::VARCHAR;
    }

    static unique_ptr<Catalog> AirportCatalogAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                                    AttachedDatabase &db, const string &name, AttachInfo &info,
                                                    AttachOptions &options)
    {
        string secret_name;
        string auth_token;
        string location;

        // check if we have a secret provided
        for (auto &entry : info.options)
        {
            auto lower_name = StringUtil::Lower(entry.first);
            if (lower_name == "type")
            {
                continue;
            }
            else if (lower_name == "secret")
            {
                secret_name = entry.second.ToString();
            }
            else if (lower_name == "auth_token")
            {
                auth_token = entry.second.ToString();
            }
            else if (lower_name == "location")
            {
                location = entry.second.ToString();
            }
            else
            {
                throw BinderException("Unrecognized option for Airport ATTACH: %s", entry.first);
            }
        }

        auth_token = AirportAuthTokenForLocation(context, location, secret_name, auth_token);

        if (location.empty())
        {
            throw BinderException("No location provided for Airport ATTACH.");
        }

        return make_uniq<AirportCatalog>(db, info.path, options.access_mode, AirportAttachParameters(location, auth_token, secret_name, ""));
    }

    static unique_ptr<TransactionManager> CreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info, AttachedDatabase &db,
                                                                   Catalog &catalog)
    {
        auto &airport_catalog = catalog.Cast<AirportCatalog>();
        return make_uniq<AirportTransactionManager>(db, airport_catalog);
    }

    class AirportCatalogStorageExtension : public StorageExtension
    {
    public:
        AirportCatalogStorageExtension()
        {
            attach = AirportCatalogAttach;
            create_transaction_manager = CreateTransactionManager;
        }
    };

    static inline void get_user_agent(DataChunk &args, ExpressionState &state, Vector &result)
    {
        D_ASSERT(args.ColumnCount() == 0);
        Value val(airport_user_agent());
        result.Reference(val);
    }

    static inline void get_airport_version(DataChunk &args, ExpressionState &state, Vector &result)
    {
        D_ASSERT(args.ColumnCount() == 0);
        Value val(AIRPORT_EXTENSION_VERSION);
        result.Reference(val);
    }

    static void AirportAddSimpleFunctions(ExtensionLoader &loader)
    {
        loader.RegisterFunction(
            ScalarFunction(
                "airport_user_agent",
                {},
                LogicalType::VARCHAR,
                get_user_agent));

        loader.RegisterFunction(
            ScalarFunction(
                "airport_version",
                {},
                LogicalType::VARCHAR,
                get_airport_version));
    }

    static void RegisterTableMacro(ExtensionLoader &loader, const string &name, const string &query,
                                   const vector<string> &params, const child_list_t<Value> &named_params)
    {
        Parser parser;
        parser.ParseQuery(query);
        const auto &stmt = parser.statements.back();
        auto &node = stmt->Cast<SelectStatement>().node;

        auto func = make_uniq<TableMacroFunction>(std::move(node));
        for (auto &param : params)
        {
            func->parameters.push_back(make_uniq<ColumnRefExpression>(param));
        }

        for (auto &param : named_params)
        {
            func->default_parameters[param.first] = make_uniq<ConstantExpression>(param.second);
        }

        CreateMacroInfo info(CatalogType::TABLE_MACRO_ENTRY);
        info.schema = DEFAULT_SCHEMA;
        info.name = name;
        info.temporary = true;
        info.internal = true;
        info.macros.push_back(std::move(func));

        loader.RegisterFunction(info);
    }

    static void AirportAddListDatabasesMacro(ExtensionLoader &loader)
    {
        child_list_t<Value> named_params = {
            //            {"auth_token", Value()},
            //            {"secret", Value()},
            // {"headers", Value()},
        };

        RegisterTableMacro(
            loader,
            "airport_databases",
            "select * from airport_take_flight(server_location, ['__databases'])",
            //            "select * from airport_take_flight(server_location, ['__databases'], auth_token=auth_token, secret=secret, headers=headers)",
            {"server_location"},
            named_params);
    }

    static void SendTelemetry(shared_ptr<DatabaseInstance> db)
    {

        // Initialize the telemetry sender
        auto doc = yyjson_mut_doc_new(nullptr);

        auto result_obj = yyjson_mut_obj(doc);
        yyjson_mut_doc_set_root(doc, result_obj);

        auto user_agent = airport_user_agent();
        auto platform = DuckDB::Platform();
        yyjson_mut_obj_add_str(doc, result_obj, "extension_name", "airport");
        yyjson_mut_obj_add_str(doc, result_obj, "airport_version", AIRPORT_EXTENSION_VERSION);
        yyjson_mut_obj_add_str(doc, result_obj, "airport_user_agent", user_agent.c_str());
        yyjson_mut_obj_add_str(doc, result_obj, "duckdb_platform", platform.c_str());
        yyjson_mut_obj_add_str(doc, result_obj, "duckdb_library_version", DuckDB::LibraryVersion());
        yyjson_mut_obj_add_str(doc, result_obj, "duckdb_release_codename", DuckDB::ReleaseCodename());
        yyjson_mut_obj_add_str(doc, result_obj, "duckdb_source_id", DuckDB::SourceID());

        size_t telemetry_len;
        auto telemetry_data = yyjson_mut_val_write_opts(
            result_obj,
            AirportJSONCommon::WRITE_FLAG,
            NULL, &telemetry_len, nullptr);

        if (telemetry_data == nullptr)
        {
            throw SerializationException("Failed to serialize telemetry data.");
        }

        auto telemetry_string = string(telemetry_data, (size_t)telemetry_len);

        // Send request asynchronously
        AirportTelemetrySender::sendRequestAsync(db, telemetry_string);
    }

    static void LoadInternal(ExtensionLoader &loader)
    {
        ExtensionHelper::AutoLoadExtension(loader.GetDatabaseInstance(), "httpfs");
        if (!loader.GetDatabaseInstance().ExtensionIsLoaded("httpfs"))
        {
            throw MissingExtensionException("The airport extension requires the httpfs extension to be loaded!");
        }

        AirportAddListFlightsFunction(loader);
        AirportAddTakeFlightFunction(loader);
        AirportAddSimpleFunctions(loader);
        AirportAddActionFlightFunction(loader);

        // So to create a new macro for airport_list_databases
        // that calls airport_take_flight with a fixed flight descriptor
        // of PATH /__databases

        AirportAddListDatabasesMacro(loader);

        SecretType secret_type;
        secret_type.name = "airport";
        secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
        secret_type.default_provider = "config";

        loader.RegisterSecretType(secret_type);

        CreateSecretFunction airport_secret_function = {"airport", "config", CreateAirportSecretFunction};
        AirportSetSecretParameters(airport_secret_function);
        loader.RegisterFunction(airport_secret_function);

        auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
        config.storage_extensions["airport"] = make_uniq<AirportCatalogStorageExtension>();

        OptimizerExtension airport_optimizer;
        airport_optimizer.optimize_function = AirportOptimizer::Optimize;
        config.optimizer_extensions.push_back(std::move(airport_optimizer));

        auto &log_manager = loader.GetDatabaseInstance().GetLogManager();
        log_manager.RegisterLogType(make_uniq<AirportLogType>());

        SendTelemetry(loader.GetDatabaseInstance().shared_from_this());
    }

    void AirportExtension::Load(ExtensionLoader &loader)
    {
        LoadInternal(loader);
    }
    std::string AirportExtension::Name()
    {
        return "airport";
    }

    std::string AirportExtension::Version() const
    {
        return "user-agent=" + airport_user_agent() + ",client=" + AIRPORT_EXTENSION_VERSION;
    }

} // namespace duckdb

extern "C"
{
    DUCKDB_CPP_EXTENSION_ENTRY(airport, loader)
    {
        duckdb::LoadInternal(loader);
    }
}
