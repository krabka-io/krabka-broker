Feature: Kafka remote metadata topic creation policy
  Scenario Outline: Topic cleanup and ISR settings match the requested metadata policy
    Given metadata topic compaction <compacted> and minimum ISR <minimum>
    When metadata topic creation configs are built
    Then metadata topic creation uses <policy> cleanup with minimum ISR <minimum> and unlimited retention

    Examples:
      | compacted | minimum | policy  |
      | false     | 2       | delete  |
      | false     | 1       | delete  |
      | true      | 2       | compact |

  Scenario: An unset minimum ISR does not override the cluster default
    Given metadata topic compaction false and minimum ISR cluster-default
    When metadata topic creation configs are built
    Then metadata topic creation leaves minimum ISR to the cluster
