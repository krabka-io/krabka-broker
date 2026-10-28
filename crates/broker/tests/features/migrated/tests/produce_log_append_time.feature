Feature: Timestamp policies govern append clocks and validation windows
  Scenario: Append time replaces the producer timestamp in responses and storage
    Given topic audit uses the timestamp policy
      | key | value |
      | message.timestamp.type | LogAppendTime |
    When a producer appends a record timestamped 1000
    Then the accepted response reports the broker's append clock reading
    And timestamp lookup observes the stored append timestamp and nothing after it

  Scenario: Create time retains the producer timestamp
    Given topic orders uses the timestamp policy
      | key | value |
    When a producer appends a record timestamped 1000
    Then the accepted CreateTime response reports no append timestamp
    And timestamp lookup retains the producer's original timestamp

  Scenario: Create time rejects an expired producer timestamp
    Given topic metrics uses the timestamp policy
      | key | value |
      | message.timestamp.before.max.ms | 3600000 |
    When a producer appends a record timestamped 1000
    Then the batch is refused with the complete invalid timestamp response
    And the refused batch leaves the log empty
    When a producer appends a record timestamped now
    Then a timestamp inside the configured window is accepted at offset zero

  Scenario: Create time rejects a future producer timestamp
    Given topic forecasts uses the timestamp policy
      | key | value |
      | message.timestamp.after.max.ms | 3600000 |
    When a producer appends a record timestamped two-hours-ahead
    Then the batch is refused with the complete invalid timestamp response
    When a producer appends a record timestamped now
    Then a timestamp inside the configured window is accepted at offset zero

  Scenario: Append time accepts timestamps outside the CreateTime window
    Given topic traces uses the timestamp policy
      | key | value |
      | message.timestamp.type | LogAppendTime |
      | message.timestamp.before.max.ms | 3600000 |
    When a producer appends a record timestamped 1000
    Then the accepted response reports the broker's append clock reading
