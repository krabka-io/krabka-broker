Feature: SCRAM credential iteration ceiling
  Kafka permits 16384 iterations and rejects a count above that ceiling
  without staging any credential mutation.

  Background:
    Given the SCRAM alteration has cluster ALTER permission

  Scenario Outline: Credential upsertions respect Kafka's inclusive iteration maximum
    Given alice supplies a SCRAM-SHA-256 credential upsertion with <iterations> iterations
    When the controller validates the SCRAM credential upsertion
    Then alice's SCRAM alteration returns error <code> with message <message>
    And exactly <records> credential metadata records are staged

    Examples:
      | iterations | code | message               | records |
      | 16384      | 0    | null                  | 1       |
      | 16385      | 93   | "Too many iterations" | 0       |
