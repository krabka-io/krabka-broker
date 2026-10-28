Feature: FindCoordinator group denials use Kafka response evolution
  Scenario Outline: Denied group lookups preserve the version-specific coordinator response
    Given the group coordinator lookup for g is denied at API version <version>
    When alice requests the group coordinator
    Then the coordinator refusal uses the <shape> Kafka response shape
    Examples:
      | version | shape                                    |
      | 4       | per-key without message                  |
      | 3       | legacy with default message and no node   |
