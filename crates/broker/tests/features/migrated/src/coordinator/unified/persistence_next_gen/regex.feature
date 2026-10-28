Feature: src coordinator unified persistence next gen regex

  @regex_value
  Scenario: Persist a resolved expression contains topics a and bc at version 7 and timestamp 1700000000000
    Given a resolved expression contains topics a and bc at version 7 and timestamp 1700000000000
    When the coordinator persists the resolved consumer subscription in the compacted group log
    Then the persisted resolved consumer subscription matches Kafka schema bytes including compact lengths and tagged trailers
