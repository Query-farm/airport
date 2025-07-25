#pragma once

#include <vector>
#include <string>
#include "arrow/flight/client.h"
#include "airport_flight_stream.hpp"

namespace duckdb
{
  const std::string airport_user_agent() noexcept;
  void airport_add_standard_headers(arrow::flight::FlightCallOptions &options, const std::string &server_location) noexcept;
  void airport_add_authorization_header(arrow::flight::FlightCallOptions &options, const std::string &auth_token) noexcept;
  void airport_add_flight_path_header(arrow::flight::FlightCallOptions &options,
                                      const arrow::flight::FlightDescriptor &descriptor);
  void airport_add_trace_id_header(arrow::flight::FlightCallOptions &options,
                                   const string &trace_id);

  void airport_add_normal_headers(arrow::flight::FlightCallOptions &options,
                                  const AirportTakeFlightParameters &params,
                                  const std::string &trace_id,
                                  const std::optional<arrow::flight::FlightDescriptor> &descriptor = std::nullopt);

  // Generate a random id that is used for request tracking.
  std::string airport_trace_id();

}