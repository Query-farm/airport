#include "airport_rpc.hpp"
#include "airport_extension.hpp"
#include "airport_macros.hpp"
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/buffer.h>

namespace duckdb
{

  std::unique_ptr<arrow::flight::Result> AirportCallAction(std::shared_ptr<arrow::flight::FlightClient> flight_client,
                                                           const arrow::flight::FlightCallOptions call_options,
                                                           const arrow::flight::Action &action,
                                                           const std::string &server_location,
                                                           bool want_result)
  {
    auto &op_name = action.type;
    AIRPORT_ASSIGN_OR_RAISE_LOCATION(auto action_results,
                                     flight_client->DoAction(call_options, action),
                                     server_location,
                                     (op_name + ": invoke"));

    std::unique_ptr<arrow::flight::Result> results_buffer = nullptr;
    if (want_result)
    {
      AIRPORT_ASSIGN_OR_RAISE_LOCATION(results_buffer,
                                       action_results->Next(),
                                       server_location,
                                       (op_name + ": reading result"));
    }

    AIRPORT_ARROW_ASSERT_OK_LOCATION(action_results->Drain(),
                                     server_location,
                                     (op_name + ": drain action result stream"));

    return results_buffer;
  }
}