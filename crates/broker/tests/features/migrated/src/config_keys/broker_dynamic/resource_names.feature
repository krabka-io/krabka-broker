Feature: Kafka BROKER resource addressing
  An empty resource names the cluster default; a numeric resource must identify
  the broker serving the alteration, and invalid names retain Kafka's exact error.

  Scenario: An empty resource names the cluster default
    Given broker 1 receives the quoted BROKER resource name ""
    When the BROKER resource name is resolved
    Then the BROKER resource targets the cluster default

  Scenario: A local resource names the serving broker
    Given broker 1 receives the quoted BROKER resource name "1"
    When the BROKER resource name is resolved
    Then the BROKER resource targets broker 1

  Scenario Outline: Other or malformed broker names are rejected precisely
    Given broker 1 receives the quoted BROKER resource name <name>
    When the BROKER resource name is resolved
    Then the BROKER resource is rejected with INVALID_REQUEST and message <message>

    Examples:
      | name | message |
      | "2" | Unexpected broker id, expected 1, but received 2 |
      | "-1" | Unexpected broker id, expected 1, but received -1 |
      | "abc" | Node id must be an integer, but it is: abc |
