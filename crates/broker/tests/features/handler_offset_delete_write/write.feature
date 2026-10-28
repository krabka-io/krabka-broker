Feature: Offset deletion translates storage failures into coordinator responses
  Scenario Outline: Kafka tombstone errors retain their coordinator meaning
    Given writing an offset deletion tombstone fails with Kafka error <write_error>
    When the offset deletion maps the write failure to its operation response
    Then the offset deletion response error is <expected>
    Examples:
      | failure                 | write_error | expected |
      | network exception       | 13          | 14       |
      | unknown topic           | 3           | 15       |
      | corrupt message         | 2           | 2        |
      | not enough replicas     | 19          | 15       |
      | request timed out       | 7           | 15       |
      | not leader              | 6           | 16       |
      | kafka storage error     | 56          | 16       |
      | message too large       | 10          | -1       |
      | record list too large   | 18          | -1       |
      | unknown server error    | -1          | -1       |
