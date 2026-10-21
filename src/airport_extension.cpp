#define DUCKDB_EXTENSION_MAIN

#include "airport_extension.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/extension_load_options.hpp"
#include "duckdb/main/extension_repository_manager.hpp"

// For DuckDB 2.0 and later this extension is distributed from the Query.Farm extension repository. The build published to the
// DuckDB community repository only keeps the name. When the user has already added the Query.Farm repository and
// installed the extension from it, loading this build loads that one; otherwise it explains the remaining steps.
// It never adds the repository or installs anything itself: trusting a repository is the user's decision.

namespace duckdb {

static constexpr const char *EXTENSION_NAME = "airport";
static constexpr const char *EXTENSION_VERSION = "2026102001";
static constexpr const char *REPOSITORY_NAME = "query_farm";
static constexpr const char *REPOSITORY_URL = "https://duckdb-extensions.query.farm";
static constexpr const char *REPOSITORY_HTTP_URL = "http://duckdb-extensions.query.farm";
// sha256 fingerprints of the Query.Farm repository signing keys (primary and standby)
static constexpr const char *REPOSITORY_KEY_FINGERPRINTS[] = {
    "sha256:b293ba6145db8f51f301618f59a720cb8c0c21e7efc6119aab43ea5c708947b5",
    "sha256:6959de1c16b783b14d4c904f69361811f737017d79ea7078729a74ec2f7f7753"};

static string MovedPreamble() {
	return StringUtil::Format("The %s extension for DuckDB 2.0 and later is distributed from the Query.Farm extension repository, "
	                          "not the DuckDB community repository.\n\n",
	                          EXTENSION_NAME);
}

static string MoreInformation() {
	return StringUtil::Format("\n\nMore information: https://query.farm/products/extensions/%s/", EXTENSION_NAME);
}

static bool IsInstalledFromRepository(DatabaseInstance &db, FileSystem &fs) {
	for (auto &directory : ExtensionHelper::GetExtensionDirectoryPath(db, fs)) {
		auto path = fs.JoinPath(fs.JoinPath(fs.JoinPath(directory, "repositories"), REPOSITORY_NAME),
		                        string(EXTENSION_NAME) + ".duckdb_extension");
		if (fs.FileExists(path)) {
			return true;
		}
	}
	return false;
}

// A repository named query_farm is only used when it points at the Query.Farm prefix and pins a Query.Farm signing key.
static bool IsQueryFarmRepository(const ExtensionRepository &repository) {
	auto prefix = StringUtil::Lower(repository.path);
	while (StringUtil::EndsWith(prefix, "/")) {
		prefix.pop_back();
	}
	if (prefix != REPOSITORY_URL && prefix != REPOSITORY_HTTP_URL) {
		return false;
	}
	for (auto &key : repository.public_keys) {
		auto fingerprint = ExtensionRepositoryManager::GetPublicKeyFingerprint(key);
		for (auto trusted : REPOSITORY_KEY_FINGERPRINTS) {
			if (fingerprint == trusted) {
				return true;
			}
		}
	}
	return false;
}

static void LoadFromQueryFarmOrExplain(DatabaseInstance &db) {
	auto &fs = FileSystem::GetFileSystem(db);
	ExtensionRepository repository;
	bool has_repository = ExtensionRepositoryManager::TryGetRepository(db, fs, REPOSITORY_NAME, repository);

	if (!has_repository) {
		throw MissingExtensionException(
		    "%sAdd the repository and install the extension once:\n"
		    "\n"
		    "    SET allow_extension_repositories = 'allowed';\n"
		    "    CREATE EXTENSION REPOSITORY IF NOT EXISTS %s WITH PREFIX '%s';\n"
		    "    INSTALL %s FROM %s;\n"
		    "\n"
		    "Then load it in each session with:\n"
		    "\n"
		    "    LOAD %s FROM %s;%s",
		    MovedPreamble(), REPOSITORY_NAME, REPOSITORY_URL, EXTENSION_NAME, REPOSITORY_NAME, EXTENSION_NAME,
		    REPOSITORY_NAME, MoreInformation());
	}
	if (!IsQueryFarmRepository(repository)) {
		throw MissingExtensionException(
		    "%sA repository named %s is configured with the prefix '%s', but it is not the Query.Farm extension "
		    "repository: it points somewhere else or does not use the Query.Farm signing keys, so the extension is not "
		    "loaded from it. Replace it with the Query.Farm repository and reinstall the extension:\n"
		    "\n"
		    "    SET allow_extension_repositories = 'allowed';\n"
		    "    CREATE OR REPLACE EXTENSION REPOSITORY %s WITH PREFIX '%s';\n"
		    "    FORCE INSTALL %s FROM %s;\n"
		    "\n"
		    "Then load it in each session with:\n"
		    "\n"
		    "    LOAD %s FROM %s;%s",
		    MovedPreamble(), REPOSITORY_NAME, repository.path, REPOSITORY_NAME, REPOSITORY_URL, EXTENSION_NAME,
		    REPOSITORY_NAME, EXTENSION_NAME, REPOSITORY_NAME, MoreInformation());
	}
	if (ExtensionRepositoryManager::GetAccess(db) == ExtensionRepositoryAccess::FORBIDDEN) {
		throw MissingExtensionException("%sThe %s repository is configured, but allow_extension_repositories is set to "
		                                "'forbidden', so extensions from it cannot be loaded.%s",
		                                MovedPreamble(), REPOSITORY_NAME, MoreInformation());
	}
	if (!IsInstalledFromRepository(db, fs)) {
		throw MissingExtensionException("%sThe %s repository is already configured. Install the extension from it once:\n"
		                                "\n"
		                                "    SET allow_extension_repositories = 'allowed';\n"
		                                "    INSTALL %s FROM %s;\n"
		                                "\n"
		                                "Then load it in each session with:\n"
		                                "\n"
		                                "    LOAD %s FROM %s;%s",
		                                MovedPreamble(), REPOSITORY_NAME, EXTENSION_NAME, REPOSITORY_NAME,
		                                EXTENSION_NAME, REPOSITORY_NAME, MoreInformation());
	}

	// The repository is trusted and the extension is installed from it: load that build. DuckDB holds the load lock
	// for this name while this stub initializes, so the real extension is loaded under an alias; its functions are
	// registered as usual.
	ExtensionLoadOptions options(EXTENSION_NAME);
	options.repository = REPOSITORY_NAME;
	options.alias = Identifier(string(EXTENSION_NAME) + "_" + REPOSITORY_NAME);
	try {
		ExtensionHelper::LoadExternalExtension(db, fs, options);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		if (error.Type() != ExceptionType::IO || !StringUtil::Contains(error.RawMessage(), "signature")) {
			throw;
		}
		throw MissingExtensionException(
		    "%sThe %s extension installed from the %s repository failed signature verification. The file may be "
		    "damaged, or it was signed with a key that this repository definition does not trust. Reinstall it:\n"
		    "\n"
		    "    SET allow_extension_repositories = 'allowed';\n"
		    "    FORCE INSTALL %s FROM %s;\n"
		    "\n"
		    "If loading still fails the same way, refresh the repository's signing keys and reinstall:\n"
		    "\n"
		    "    CREATE OR REPLACE EXTENSION REPOSITORY %s WITH PREFIX '%s';\n"
		    "    FORCE INSTALL %s FROM %s;\n"
		    "\n"
		    "Do not use -unsigned to work around this.%s",
		    MovedPreamble(), EXTENSION_NAME, REPOSITORY_NAME, EXTENSION_NAME, REPOSITORY_NAME, REPOSITORY_NAME,
		    REPOSITORY_URL, EXTENSION_NAME, REPOSITORY_NAME, MoreInformation());
	}
}

void AirportExtension::Load(ExtensionLoader &loader) {
	LoadFromQueryFarmOrExplain(loader.GetDatabaseInstance());
}

std::string AirportExtension::Name() {
	return EXTENSION_NAME;
}

std::string AirportExtension::Version() const {
	return EXTENSION_VERSION;
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(airport, loader) {
	duckdb::LoadFromQueryFarmOrExplain(loader.GetDatabaseInstance());
}
}
