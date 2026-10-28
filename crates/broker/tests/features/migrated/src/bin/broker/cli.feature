Feature: Operator-supplied cluster identity
  Operators can copy the cluster ID from Kafka metadata or Java tooling.
  Both representations must configure the same UUID; malformed IDs prevent startup.

  Scenario Outline: Operators can use either supported cluster ID representation
    Given the operator supplies cluster ID <value>
    When the broker parses the cluster ID command line
    Then the configured cluster UUID is 01020304-0506-0708-090a-0b0c0d0e0f10

    Examples:
      | value                                |
      | AQIDBAUGBwgJCgsMDQ4PEA               |
      | 01020304-0506-0708-090a-0b0c0d0e0f10 |

  Scenario: An invalid cluster ID prevents startup
    Given the operator supplies cluster ID not-a-cluster-id
    When the broker parses the cluster ID command line
    Then the malformed cluster ID is rejected
