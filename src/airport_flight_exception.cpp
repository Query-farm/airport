#include "duckdb.hpp"
#include "airport_flight_exception.hpp"

#include "airport_extension.hpp"
#include "duckdb.hpp"

// Arrow includes.
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/buffer.h>
#include "yyjson.hpp"
#include "airport_secrets.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb
{

  namespace
  {
    std::string joinWithSlash(const std::vector<std::string> &vec)
    {
      if (vec.empty())
        return ""; // Handle empty vector case

      std::ostringstream result;
      for (size_t i = 0; i < vec.size(); ++i)
      {
        result << vec[i];
        if (i != vec.size() - 1)
        {
          result << '/';
        }
      }
      return result.str();
    }

    // Function to split string based on a delimiter
    std::vector<std::string> split(const std::string &input, const std::string &delimiter)
    {
      std::vector<std::string> parts;
      size_t pos = 0;
      size_t start = 0;

      while ((pos = input.find(delimiter, start)) != std::string::npos)
      {
        parts.push_back(input.substr(start, pos - start));
        start = pos + delimiter.length();
      }

      // Add the last part after the final delimiter
      parts.push_back(input.substr(start));

      return parts;
    }

    void extract_grpc_debug_context(const string &value, std::unordered_map<string, string> &result)
    {
      if (value.find("gRPC client debug context: ") != std::string::npos)
      {
        // Extract the gRPC context from the error message
        std::vector<std::string> parts = split(value, "gRPC client debug context: ");
        if (parts.size() > 1)
        {
          result["grpc_context"] = parts[1]; // Store the gRPC context in the result map
        }
      }
    }

    string extract_grpc_context_prefix(const string &value)
    {
      if (value.find("gRPC client debug context: ") != std::string::npos)
      {
        // Extract the gRPC context from the error message
        std::vector<std::string> parts = split(value, "gRPC client debug context: ");
        if (parts.size() > 1)
        {
          return parts[0]; // Return the part before the gRPC context
        }
      }
      return value; // Return the original value if no gRPC context is found
    }

    string build_error_message(const string &location, const string &descriptor, const string &msg, const string &status)
    {
      // Handle other descriptor types if needed
      string descriptor_joiner = "/";
      if (location.back() == '/')
      {
        descriptor_joiner = "";
      }

      string extra_msg = "";
      if (!msg.empty())
      {
        extra_msg = msg + " ";
      }

      return "Airport @ " + location + descriptor_joiner + descriptor + ": " + extra_msg + status;
    }

    string descriptor_to_string(const flight::FlightDescriptor &descriptor)
    {
      if (descriptor.type == flight::FlightDescriptor::DescriptorType::PATH)
      {
        return joinWithSlash(descriptor.path);
      }
      else if (descriptor.type == flight::FlightDescriptor::DescriptorType::CMD)
      {
        return "'" + descriptor.cmd + "'";
      }
      else
      {
        throw NotImplementedException("Unsupported Arrow Flight descriptor type");
      }
    }
  }

  unordered_map<string, string> AirportFlightException::extract_extra_info(const string &status, const unordered_map<string, string> &extra_info)
  {
    auto arrow_status_string = status;

    // Create an unordered_map to store the key-value pairs
    std::unordered_map<std::string, std::string> result = extra_info;

    extract_grpc_debug_context(arrow_status_string, result);

    return result;
  }

  unordered_map<string, string> AirportFlightException::extract_extra_info(const arrow::Status &status, const unordered_map<string, string> &extra_info)
  {
    auto arrow_status_string = status.message();

    // Create an unordered_map to store the key-value pairs
    std::unordered_map<std::string, std::string> result = extra_info;

    extract_grpc_debug_context(arrow_status_string, result);

    std::shared_ptr<flight::FlightStatusDetail> flight_status = flight::FlightStatusDetail::UnwrapStatus(status);
    // The extra info is just some bytes, but in my servers its going to be a json document
    // that can only have string values
    if (flight_status != nullptr)
    {
      auto extra_info_string = flight_status->extra_info();
      if (!extra_info_string.empty())
      {
        // Parse the JSON string
        yyjson_doc *doc = yyjson_read(extra_info_string.c_str(), extra_info_string.size(), 0);
        if (doc)
        {
          // Get the root of the JSON document
          yyjson_val *root = yyjson_doc_get_root(doc);

          // Ensure the root is an object (dictionary)
          if (root && yyjson_is_obj(root))
          {
            size_t idx, max;
            yyjson_val *key, *value;
            yyjson_obj_foreach(root, idx, max, key, value)
            {
              if (yyjson_is_str(key) && yyjson_is_str(value))
              {
                // Add the key-value pair to the unordered_map
                result[yyjson_get_str(key)] = yyjson_get_str(value);
              }
            }
          }
        }
        // Free the JSON document after parsing
        yyjson_doc_free(doc);
      }
    }
    return result;
  }

  string AirportFlightException::produce_flight_error_message(const string &location, const flight::FlightDescriptor &descriptor, const arrow::Status &status, const string &msg)
  {
    // So its kind of silly to have the grpc context
    auto arrow_status_string = extract_grpc_context_prefix(status.message());

    string descriptor_string = descriptor_to_string(descriptor);

    return build_error_message(location, descriptor_string, msg, arrow_status_string);
  }

  string AirportFlightException::produce_flight_error_message(const string &location, const flight::FlightDescriptor &descriptor, const string &status, const string &msg)
  {
    // So its kind of silly to have the grpc context
    auto arrow_status_string = extract_grpc_context_prefix(status);

    string descriptor_string = descriptor_to_string(descriptor);

    return build_error_message(location, descriptor_string, msg, arrow_status_string);
  }

  string AirportFlightException::produce_flight_error_message(const string &location, const arrow::Status &status, const string &msg)
  {
    // So its kind of silly to have the grpc context
    auto arrow_status_string = extract_grpc_context_prefix(status.message());

    return build_error_message(location, "", msg, arrow_status_string);
  }

  string AirportFlightException::produce_flight_error_message(const string &location, const string &msg)
  {
    // So its kind of silly to have the grpc context
    return build_error_message(location, "", msg, "");
  }

  AirportFlightException::AirportFlightException(const string &location, const arrow::Status &status, const string &msg) : Exception(ExceptionType::IO, produce_flight_error_message(location, status, msg), extract_extra_info(status, {}))
  {
  }

  AirportFlightException::AirportFlightException(const string &location, const string &msg) : Exception(ExceptionType::IO, produce_flight_error_message(location, msg))
  {
  }

  AirportFlightException::AirportFlightException(const string &location, const arrow::Status &status, const string &msg, const unordered_map<string, string> &extra_info)
      : Exception(ExceptionType::IO, produce_flight_error_message(location, status, msg), extract_extra_info(status, extra_info))
  {
  }

  AirportFlightException::AirportFlightException(const string &location, const flight::FlightDescriptor &descriptor, const string &status, const string &msg) : Exception(ExceptionType::IO, produce_flight_error_message(location, descriptor, status, msg), extract_extra_info(status, {}))
  {
  }

  AirportFlightException::AirportFlightException(const string &location, const flight::FlightDescriptor &descriptor, const arrow::Status &status, const string &msg) : Exception(ExceptionType::IO, produce_flight_error_message(location, descriptor, status, msg), extract_extra_info(status, {}))
  {
  }

  AirportFlightException::AirportFlightException(const string &location, const flight::FlightDescriptor &descriptor, const arrow::Status &status, const string &msg, const unordered_map<string, string> &extra_info)
      : Exception(ExceptionType::IO, produce_flight_error_message(location, descriptor, status, msg), extract_extra_info(status, extra_info))
  {
  }
}