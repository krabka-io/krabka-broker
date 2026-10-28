Feature: Kafka 4.3.1 released API version catalogue
  Released API limits bound what both listeners advertise by default, independently
  of newer trunk schemas. Unsupported API keys do not acquire a guessed limit.

  Scenario: Published keys are unique and expose valid version ranges
    Given every API key published in the Kafka 4.3.1 catalogue
    When every published API is resolved through the release lookup
    Then the release exposes 89 unique ascending API keys with nonempty version ranges

  Scenario Outline: Released API maximum lookup recognizes known and unsupported keys
    Given an API key <key> requested from the Kafka 4.3.1 catalogue
    When the released maximum API version is looked up
    Then the released maximum API version is <version>

    Examples:
      | key  | version     |
      | 18   | 4           |
      | 22   | 5           |
      | 28   | 5           |
      | 88   | 0           |
      | 89   | 0           |
      | 92   | 0           |
      | 93   | unavailable |
      | 94   | unavailable |
      | 1020 | unavailable |

  Scenario: Fetch retains its complete released version range
    Given an API key 1 requested from the Kafka 4.3.1 catalogue
    When the released API version range is looked up
    Then the released Fetch API spans versions 4 through 18
