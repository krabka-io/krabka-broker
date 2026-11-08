Feature: Flexible assignment epoch wire compatibility
  Scenario Outline: Epoch values preserve Kafka bytes and reject truncation
    Given assignment epoch <epoch> at timestamp <timestamp>
    And Kafka epoch value bytes <golden>
    When the epoch value and every shorter byte prefix are passed through the codec
    Then the Kafka bytes round trip and every truncated prefix is rejected

    Examples:
      | epoch | timestamp | golden                                      |
      | 7     | 0         | 0000 00000007 00                            |
      | 7     | 42        | 0000 00000007 01 00 08 000000000000002a       |
      | -1    | -1        | 0000 ffffffff 01 00 08 ffffffffffffffff       |
