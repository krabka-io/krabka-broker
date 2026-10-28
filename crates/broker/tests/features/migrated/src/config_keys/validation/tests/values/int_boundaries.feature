Feature: Kafka topic INT keys preserve signed 32-bit domains
  A valid wider LONG never sneaks into an INT topic key.

  Scenario Outline: Kafka topic INT keys preserve signed 32-bit domains
    Given a topic alteration supplies key <key> and quoted value <value>
    When the released per-key topic validator reads the supplied value
    Then the supplied topic value is accepted <accepted>

    Examples:
      | key | value | accepted |
      | segment.bytes | "2147483647" | true |
      | segment.bytes | "2147483648" | false |
      | segment.bytes | "9223372036854775807" | false |
      | min.insync.replicas | "2147483647" | true |
      | min.insync.replicas | "2147483648" | false |
      | min.insync.replicas | "0" | false |
      | min.insync.replicas | "-1" | false |
