#pragma once

#include "duckdb.hpp"
#include <memory>
#include <string>

// Forward declarations for Arrow Flight types
namespace arrow
{
  namespace flight
  {
    class FlightClient;
    class FlightCallOptions;
    class Action;
    class Result;
  } // namespace flight
} // namespace arrow

namespace duckdb
{

  /**
   * Call an action on an Arrow Flight server and return the result.
   *
   * @param flight_client The Flight client to use for the call
   * @param call_options The call options for the Flight request
   * @param action The action to execute
   * @param server_location The server location for error reporting
   * @param op_name The operation name for error reporting
   * @return A unique pointer to the action result
   */
  std::unique_ptr<arrow::flight::Result> AirportCallAction(
      std::shared_ptr<arrow::flight::FlightClient> flight_client,
      const arrow::flight::FlightCallOptions call_options,
      const arrow::flight::Action &action,
      const std::string &server_location,
      bool want_result = true);

} // namespace duckdb
