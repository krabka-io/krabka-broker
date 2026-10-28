Feature: Kafka segment bytes has a one-MiB floor
  Public segment sizing honors Kafka INT parsing and its one-MiB floor.

  Scenario Outline: Kafka segment bytes has a one-MiB floor
    Given a topic alteration supplies key segment.bytes and quoted value <value>
    When the released per-key topic validator reads the supplied value
    Then the supplied topic value is accepted <accepted>

    Examples:
      | value | accepted |
      | "0" | false |
      | "1" | false |
      | "14" | false |
      | "1048575" | false |
      | "1048576" | true |
      | "1073741824" | true |
      | "not-a-number" | false |
