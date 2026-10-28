Feature: Transaction marker response codes determine completion and retry behavior
  The coordinator follows Kafka's completion-handler policy for every relevant partition error.

  Scenario Outline: Classify marker response <error>
    Given a WriteTxnMarkers partition response has Kafka error code <code>
    When the transaction coordinator classifies the marker response
    Then the marker response decision is <decision>

    Examples:
      | code | error | decision |
      | 0 | NONE | Written |
      | 43 | UNSUPPORTED_FOR_MESSAGE_FORMAT | Dropped |
      | 35 | UNSUPPORTED_VERSION | Dropped |
      | 3 | UNKNOWN_TOPIC_OR_PARTITION | Retriable |
      | 6 | NOT_LEADER_OR_FOLLOWER | Retriable |
      | 19 | NOT_ENOUGH_REPLICAS | Retriable |
      | 20 | NOT_ENOUGH_REPLICAS_AFTER_APPEND | Retriable |
      | 7 | REQUEST_TIMED_OUT | Retriable |
      | 56 | KAFKA_STORAGE_ERROR | Retriable |
      | 47 | INVALID_PRODUCER_EPOCH | Fenced |
      | 52 | TRANSACTION_COORDINATOR_FENCED | Fenced |
      | 2 | CORRUPT_MESSAGE | Unexpected |
      | 10 | MESSAGE_TOO_LARGE | Unexpected |
      | 18 | RECORD_LIST_TOO_LARGE | Unexpected |
      | 21 | INVALID_REQUIRED_ACKS | Unexpected |
      | -1 | UNKNOWN_SERVER_ERROR | Unexpected |
      | 31 | CLUSTER_AUTHORIZATION_FAILED | Unexpected |
      | 16 | NOT_COORDINATOR | Unexpected |
      | 14 | COORDINATOR_LOAD_IN_PROGRESS | Unexpected |
      | 48 | INVALID_TXN_STATE | Unexpected |
