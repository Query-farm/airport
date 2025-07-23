#pragma once

#include "duckdb/logging/logging.hpp"
#include "duckdb/logging/log_type.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb
{

  struct AirportLogType : public LogType
  {
    static constexpr const char *NAME = "Airport";
    static constexpr LogLevel LEVEL = LogLevel::LOG_INFO;

    //! Construct the log type
    AirportLogType();

    static LogicalType GetLogType();

    static string
    ConstructLogMessage(const string &event,
                        const vector<pair<string, string>> &info);
  };
}
