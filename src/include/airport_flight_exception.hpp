#pragma once

#include "duckdb/common/exception.hpp"
#include <vector>
#include <stdexcept>

#include <arrow/flight/client.h>
namespace flight = arrow::flight;

namespace duckdb
{

  // A custom exception class for Airport that can include the flight descriptions
  // and server location, it is important to surface this information on errors because
  // if multiple servers are being used, its helpful to help the user to diagnose
  // where the error is being caused.
  class AirportFlightException : public Exception
  {

  private:
    static string produce_flight_error_message(const string &location, const flight::FlightDescriptor &descriptor, const arrow::Status &status, const string &msg);
    static string produce_flight_error_message(const string &location, const flight::FlightDescriptor &descriptor, const string &status, const string &msg);
    static string produce_flight_error_message(const string &location, const arrow::Status &status, const string &msg);
    static string produce_flight_error_message(const string &location, const string &msg);

    static unordered_map<string, string> extract_extra_info(const arrow::Status &status, const unordered_map<string, string> &extra_info);
    static unordered_map<string, string> extract_extra_info(const string &status, const unordered_map<string, string> &extra_info);

  public:
    explicit AirportFlightException(const string &location, const arrow::Status &status, const string &msg);
    explicit AirportFlightException(const string &location, const string &msg);
    explicit AirportFlightException(const string &location, const arrow::Status &status, const string &msg, const unordered_map<string, string> &extra_info);

    explicit AirportFlightException(const string &location, const flight::FlightDescriptor &descriptor, const string &status, const string &msg);
    explicit AirportFlightException(const string &location, const flight::FlightDescriptor &descriptor, const arrow::Status &status, const string &msg);
    explicit AirportFlightException(const string &location, const flight::FlightDescriptor &descriptor, const arrow::Status &status, const string &msg, const unordered_map<string, string> &extra_info);
    explicit AirportFlightException(ExceptionType exception_type, const string &location, const flight::FlightDescriptor &descriptor, const arrow::Status &status, const string &msg) : Exception(exception_type, produce_flight_error_message(location, descriptor, status, msg), extract_extra_info(status, {}))
    {
    }

    explicit AirportFlightException(ExceptionType exception_type, const string &location, const arrow::Status &status, const string &msg) : Exception(exception_type, produce_flight_error_message(location, status, msg), extract_extra_info(status, {}))
    {
    }
  };
}