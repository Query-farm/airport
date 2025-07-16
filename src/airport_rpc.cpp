#include "airport_rpc.hpp"
#include "airport_extension.hpp"
#include "airport_macros.hpp"
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/buffer.h>
#include <chrono>
#include <thread>
#include <random>
#include <algorithm>

namespace duckdb
{

  std::unique_ptr<arrow::flight::Result> AirportCallAction(std::shared_ptr<arrow::flight::FlightClient> flight_client,
                                                           const arrow::flight::FlightCallOptions call_options,
                                                           const arrow::flight::Action &action,
                                                           const std::string &server_location,
                                                           bool want_result)
  {
    std::random_device rd;
    std::mt19937 gen(rd());

    auto &op_name = action.type;

    int max_retries = 5;
    std::chrono::milliseconds initial_delay{100};
    std::chrono::milliseconds max_delay{5000};
    double backoff_multiplier = 2.0;
    double jitter_factor = 0.1; // 10% jitter

    std::unique_ptr<arrow::flight::ResultStream> action_results = nullptr;

    // Since Flight servers can be busy implement exponential backoff with jitter
    // to avoid overwhelming the server with retries, if we get IO errors.

    for (int attempt = 0; attempt <= max_retries; ++attempt)
    {
      auto invoke_result = flight_client->DoAction(call_options, action);

      if (invoke_result.ok())
      {
        action_results = std::move(invoke_result).ValueUnsafe();
        break;
      }

      // If this was the last attempt, throw the exception
      if (attempt == max_retries || !invoke_result.status().IsIOError())
      {
        throw AirportFlightException(server_location, invoke_result.status(),
                                     (op_name + ": invoke"), {});
      }

      // Calculate delay with exponential backoff
      auto delay = initial_delay;
      for (int i = 0; i < attempt; ++i)
      {
        delay = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::duration<double, std::milli>(delay.count() * backoff_multiplier));
      }

      // Cap the delay at max_delay
      delay = std::min(delay, max_delay);

      // Add jitter to avoid thundering herd
      if (jitter_factor > 0)
      {
        std::uniform_real_distribution<double> jitter_dist(
            1.0 - jitter_factor, 1.0 + jitter_factor);
        delay = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::duration<double, std::milli>(delay.count() * jitter_dist(gen)));
      }

      // Sleep before retry
      std::this_thread::sleep_for(delay);
    }

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