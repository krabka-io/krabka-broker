Feature: Per-topic diskless configuration resolution
  Diskless mode is opt-in. The topic resolver follows Kafka's trimmed,
  case-insensitive boolean convention when an override is present.

  Scenario: No topic overrides leave diskless mode disabled
    Given the topic has no override map
    When the broker resolves the topic's diskless mode
    Then diskless mode is enabled false

  Scenario: An unrelated override map leaves diskless mode disabled
    Given the topic override map has no diskless key
    When the broker resolves the topic's diskless mode
    Then diskless mode is enabled false

  Scenario Outline: An explicit boolean selects the diskless mode
    Given the diskless topic override is the literal <value>
    When the broker resolves the topic's diskless mode
    Then diskless mode is enabled <enabled>

    Examples:
      | value | enabled |
      | false | false   |
      | true  | true    |

  Scenario: Surrounding whitespace and uppercase preserve a true override
    Given the diskless topic override is uppercase TRUE padded with spaces
    When the broker resolves the topic's diskless mode
    Then diskless mode is enabled true
