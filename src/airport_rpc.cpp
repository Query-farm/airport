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

  std::unique_ptr<arrow::flight::Result> AirportCallAction(
      std::shared_ptr<arrow::flight::FlightClient> flight_client,
      const arrow::flight::FlightCallOptions call_options,
      const arrow::flight::Action &action,
      const std::string &server_location,
      bool want_result)
  {
    std::random_device rd;
    std::mt19937 gen(rd());

    auto &op_name = action.type;

    int max_retries = 15;
    std::chrono::milliseconds initial_delay{100};
    std::chrono::milliseconds max_delay{5000};
    double backoff_multiplier = 2.0;
    double jitter_factor = 0.1; // 10% jitter

    std::unique_ptr<arrow::flight::Result> results_buffer = nullptr;
    std::unique_ptr<arrow::flight::ResultStream> action_results = nullptr;

    auto compute_delay = [&](int attempt)
    {
      auto delay = initial_delay;
      for (int i = 0; i < attempt; ++i)
      {
        delay = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::duration<double, std::milli>(delay.count() * backoff_multiplier));
      }
      delay = std::min(delay, max_delay);
      if (jitter_factor > 0)
      {
        std::uniform_real_distribution<double> jitter_dist(
            1.0 - jitter_factor, 1.0 + jitter_factor);
        delay = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::duration<double, std::milli>(delay.count() * jitter_dist(gen)));
      }
      return delay;
    };

    // Retry DoAction
    for (int attempt = 0; attempt <= max_retries; ++attempt)
    {
      auto invoke_result = flight_client->DoAction(call_options, action);

      if (invoke_result.ok())
      {
        action_results = std::move(invoke_result).ValueUnsafe();
        break;
      }

      if (attempt == max_retries || !invoke_result.status().IsIOError())
      {
        throw AirportFlightException(server_location, invoke_result.status(),
                                     (op_name + ": invoke"), {});
      }

      std::this_thread::sleep_for(compute_delay(attempt));
    }

    if (want_result)
    {
      // Retry action_results->Next()
      for (int attempt = 0; attempt <= max_retries; ++attempt)
      {
        auto next_result = action_results->Next();
        if (next_result.ok())
        {
          results_buffer = std::move(next_result).ValueUnsafe();
          break;
        }

        if (attempt == max_retries || !next_result.status().IsIOError())
        {
          throw AirportFlightException(server_location, next_result.status(),
                                       (op_name + ": reading result"), {});
        }

        std::this_thread::sleep_for(compute_delay(attempt));
      }
    }

    AIRPORT_ARROW_ASSERT_OK_LOCATION(action_results->Drain(),
                                     server_location,
                                     (op_name + ": drain action result stream"));

    return results_buffer;
  }

}