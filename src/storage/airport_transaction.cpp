#include "airport_extension.hpp"
#include "storage/airport_transaction.hpp"
#include "storage/airport_catalog.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "airport_request_headers.hpp"
#include "airport_macros.hpp"
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/buffer.h>
#include "airport_macros.hpp"
#include "airport_rpc.hpp"

namespace duckdb
{

  AirportTransaction::AirportTransaction(AirportCatalog &airport_catalog, TransactionManager &manager, ClientContext &context)
      : Transaction(manager, context), access_mode_(airport_catalog.access_mode()),
        catalog_name(airport_catalog.internal_name()),
        attach_parameters(airport_catalog.attach_parameters())
  {
  }

  AirportTransaction::~AirportTransaction() = default;

  struct GetTransactionIdentifierResult
  {
    std::optional<std::string> identifier;
    MSGPACK_DEFINE_MAP(identifier)
  };

  struct AirportCreateTransactionParameters
  {
    std::string catalog_name;
    MSGPACK_DEFINE_MAP(catalog_name)
  };

  std::optional<string> AirportTransaction::GetTransactionIdentifier()
  {
    auto &server_location = attach_parameters->location();
    auto flight_client = AirportAPI::FlightClientForLocation(attach_parameters->location());

    arrow::flight::FlightCallOptions call_options;
    airport_add_standard_headers(call_options, attach_parameters->location());
    airport_add_authorization_header(call_options, attach_parameters->auth_token());

    AirportCreateTransactionParameters params;
    params.catalog_name = catalog_name;
    AIRPORT_MSGPACK_ACTION_SINGLE_PARAMETER(action, "create_transaction", params);

    auto result_buffer = AirportCallAction(flight_client, call_options, action, server_location);

    AIRPORT_MSGPACK_UNPACK(
        GetTransactionIdentifierResult, result,
        (*(result_buffer->body)),
        server_location,
        "File to parse msgpack encoded create_transaction response");

    return result.identifier;
  }

  void AirportTransaction::Start()
  {
    transaction_state = AirportTransactionState::TRANSACTION_NOT_YET_STARTED;
    // Get an identifier from the server that will be passed as airport-transaction-id
    // in requests.
    identifier_ = GetTransactionIdentifier();
  }
  void AirportTransaction::Commit()
  {
    if (transaction_state == AirportTransactionState::TRANSACTION_STARTED)
    {
      transaction_state = AirportTransactionState::TRANSACTION_FINISHED;
    }
  }
  void AirportTransaction::Rollback()
  {
    if (transaction_state == AirportTransactionState::TRANSACTION_STARTED)
    {
      transaction_state = AirportTransactionState::TRANSACTION_FINISHED;
    }
  }

  AirportTransaction &AirportTransaction::Get(ClientContext &context, Catalog &catalog)
  {
    return Transaction::Get(context, catalog).Cast<AirportTransaction>();
  }

} // namespace duckdb
