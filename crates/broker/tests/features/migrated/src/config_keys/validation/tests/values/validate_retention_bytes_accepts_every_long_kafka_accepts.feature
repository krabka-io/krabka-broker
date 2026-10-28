Feature: Kafka retention bytes accepts every representable LONG
  No invented range check rejects negative Kafka LONG values or admits overflow.

  Scenario Outline: Kafka retention bytes accepts every representable LONG
    Given a topic alteration supplies key retention.bytes and quoted value <value>
    When the released per-key topic validator reads the supplied value
    Then the supplied topic value is accepted <accepted>

    Examples:
      | value | accepted |
      | "-1" | true |
      | "-2" | true |
      | "0" | true |
      | "1073741824" | true |
      | "-9223372036854775808" | true |
      | "9223372036854775807" | true |
      | "9223372036854775808" | false |
      | "abc" | false |
