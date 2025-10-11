#pragma once
#include <string>
#include "duckdb.hpp"

#if defined(_WIN32) || defined(_WIN64)
// Windows: functions are hidden by default unless exported
#define INTERNAL_FUNC
#elif defined(__GNUC__) || defined(__clang__)
// Linux / macOS: hide symbol using visibility attribute
#define INTERNAL_FUNC __attribute__((visibility("hidden")))
#else
#define INTERNAL_FUNC
#endif

namespace duckdb {
void QueryFarmSendTelemetry(ExtensionLoader &loader, const string &extension_name, const string &extension_version);
}