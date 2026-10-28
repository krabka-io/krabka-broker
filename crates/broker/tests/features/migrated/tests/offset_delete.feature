Feature: tests offset delete

  Scenario: delete offsets from empty group round trip
    Then offset delete: delete offsets from empty group round trip

  Scenario: delete offsets unknown group returns group id not found
    Then offset delete: delete offsets unknown group returns group id not found

  Scenario: delete offsets missing topic returns unknown topic or partition
    Then offset delete: delete offsets missing topic returns unknown topic or partition

  Scenario: delete offsets partition out of range returns unknown topic or partition
    Then offset delete: delete offsets partition out of range returns unknown topic or partition

  Scenario: delete offsets for subscribed topic returns group subscribed
    Then offset delete: delete offsets for subscribed topic returns group subscribed
