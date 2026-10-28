Feature: Producing legacy messages into a modern Kafka log
  Scenario: A v1 message set is stored and fetched as equivalent v2 records
    Given the legacy producer has these v1 message values
      | value |
      | alpha |
      | beta  |
      | gamma |
    When the message set is produced through the legacy records payload path
    Then the legacy produce is accepted
    When a modern consumer fetches the committed records
    Then the fetch succeeds and returns these values in one v2 batch
      | value |
      | alpha |
      | beta  |
      | gamma |

  Scenario: A corrupt but correctly framed legacy message set is rejected as invalid records
    Given the legacy producer has a framed v1 message set containing corrupt record bytes
    When the message set is produced through the legacy records payload path
    Then the corrupt legacy produce is refused with INVALID_RECORD 87
