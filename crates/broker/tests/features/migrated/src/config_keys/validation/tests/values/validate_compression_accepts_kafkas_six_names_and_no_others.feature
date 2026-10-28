Feature: Kafka compression names are exactly the six broker codec names
  Producer-side none, uppercase names, and empty values remain invalid.

  Scenario Outline: Kafka compression names are exactly the six broker codec names
    Given a topic alteration supplies key compression.type and quoted value <value>
    When the released per-key topic validator reads the supplied value
    Then the supplied topic value is accepted <accepted>

    Examples:
      | value | accepted |
      | "producer" | true |
      | "uncompressed" | true |
      | "gzip" | true |
      | "snappy" | true |
      | "lz4" | true |
      | "zstd" | true |
      | "none" | false |
      | "" | false |
      | "GZIP" | false |
      | " gzip" | true |
