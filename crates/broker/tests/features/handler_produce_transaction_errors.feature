Feature: Transaction coordinator errors in Produce partition responses

  Scenario Outline: Translate coordinator errors at protocol compatibility boundaries
    Given a transaction coordinator answer <answer> for Produce version <version>
    When the coordinator answer becomes a Produce partition error
    Then the Produce row has code <code> and message "<message>"

    Examples:
      | answer | version | code | message |
      | NONE | 11 | NONE | null |
      | PRODUCER_FENCED | 11 | INVALID_PRODUCER_EPOCH | null |
      | INVALID_PRODUCER_EPOCH | 12 | INVALID_PRODUCER_EPOCH | null |
      | TRANSACTION_ABORTABLE | 10 | INVALID_TXN_STATE | Partition was not added to the transaction |
      | TRANSACTION_ABORTABLE | 11 | TRANSACTION_ABORTABLE | null |
      | INVALID_TXN_STATE | 12 | INVALID_TXN_STATE | Partition was not added to the transaction |
      | CLUSTER_AUTHORIZATION_FAILED | 11 | INVALID_TXN_STATE | Partition was not added to the transaction |
      | NETWORK_EXCEPTION | 12 | NOT_ENOUGH_REPLICAS | Unable to verify the partition has been added to the transaction. Underlying error: NETWORK_EXCEPTION |
      | NOT_COORDINATOR | 11 | NOT_ENOUGH_REPLICAS | Unable to verify the partition has been added to the transaction. Underlying error: NOT_COORDINATOR |
      | COORDINATOR_NOT_AVAILABLE | 11 | NOT_ENOUGH_REPLICAS | Unable to verify the partition has been added to the transaction. Underlying error: COORDINATOR_NOT_AVAILABLE |
      | COORDINATOR_LOAD_IN_PROGRESS | 11 | NOT_ENOUGH_REPLICAS | Unable to verify the partition has been added to the transaction. Underlying error: COORDINATOR_LOAD_IN_PROGRESS |
      | CONCURRENT_TRANSACTIONS | 11 | NOT_ENOUGH_REPLICAS | Unable to verify the partition has been added to the transaction. Underlying error: CONCURRENT_TRANSACTIONS |
      | CONCURRENT_TRANSACTIONS | 12 | CONCURRENT_TRANSACTIONS | null |
      | INVALID_PRODUCER_ID_MAPPING | 12 | INVALID_PRODUCER_ID_MAPPING | null |
