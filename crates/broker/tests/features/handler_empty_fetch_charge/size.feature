Feature: Fetch quota accounting includes version-specific empty response overhead
  Scenario Outline: The empty response is charged according to its Kafka wire layout
    Given an empty Fetch response uses API version <version>
    When the fetch response size is charged to the consumer byte quota
    Then the empty response quota charge is <bytes> bytes
    Examples:
      | version | bytes |
      | 0       | 8     |
      | 1       | 12    |
      | 4       | 12    |
      | 7       | 18    |
      | 12      | 16    |
