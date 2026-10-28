Feature: Registry failures do not silently bypass schema validation
  Scenario Outline: a registry that cannot answer fails closed
    Given the schema registry is <availability> and fail_open is false
    And the record carries schema 42 with arbitrary body bytes
    When the producer sends the prepared record to validated
    Then schema produce to validated returns error 87 and its log end is 0

    Examples:
      | availability |
      | unconfigured |
      | unavailable  |

  Scenario: fail open admits a record the registry could not answer for
    Given the schema registry is unavailable and fail_open is true
    And the record carries schema 42 with arbitrary body bytes
    When the producer sends the prepared record to validated
    Then schema produce to validated returns error 0 and its log end is 1
