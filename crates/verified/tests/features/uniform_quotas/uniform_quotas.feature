Feature: Kafka uniform assignment quotas

  Scenario: 11 partitions over 3 members
    Given 11 partitions and 3 members
    When the uniform quota is split
    Then every member has minimum quota 3 and 2 members receive an extra partition

  Scenario: 5 partitions over 2 members
    Given 5 partitions and 2 members
    When the uniform quota is split
    Then every member has minimum quota 2 and 1 members receive an extra partition

  Scenario: 2 partitions over 3 members
    Given 2 partitions and 3 members
    When the uniform quota is split
    Then every member has minimum quota 0 and 2 members receive an extra partition

  Scenario: 6 partitions over 3 members
    Given 6 partitions and 3 members
    When the uniform quota is split
    Then every member has minimum quota 2 and 0 members receive an extra partition

  Scenario: 0 partitions over 4 members
    Given 0 partitions and 4 members
    When the uniform quota is split
    Then every member has minimum quota 0 and 0 members receive an extra partition

  Scenario: first assignment, 5 over 2
    Given minimum quota 2 with 1 extra partitions
    And members own partitions 0,0
    When homogeneous member quotas are assigned
    Then the member quotas are:
      | takes_extra | retain | fill |
      | false       | 0      | 2    |
      | true        | 0      | 3    |

  Scenario: first assignment, 2 over 3
    Given minimum quota 0 with 2 extra partitions
    And members own partitions 0,0,0
    When homogeneous member quotas are assigned
    Then the member quotas are:
      | takes_extra | retain | fill |
      | false       | 0      | 0    |
      | true        | 0      | 1    |
      | true        | 0      | 1    |

  Scenario: unbalanced previous assignment
    Given minimum quota 3 with 0 extra partitions
    And members own partitions 4,2
    When homogeneous member quotas are assigned
    Then the member quotas are:
      | takes_extra | retain | fill |
      | false       | 3      | 0    |
      | false       | 2      | 1    |

  Scenario: partitions added
    Given minimum quota 5 with 1 extra partitions
    And members own partitions 3,3
    When homogeneous member quotas are assigned
    Then the member quotas are:
      | takes_extra | retain | fill |
      | false       | 3      | 2    |
      | true        | 3      | 3    |

  Scenario: member added
    Given minimum quota 2 with 0 extra partitions
    And members own partitions 3,3,0
    When homogeneous member quotas are assigned
    Then the member quotas are:
      | takes_extra | retain | fill |
      | false       | 2      | 0    |
      | false       | 2      | 0    |
      | false       | 0      | 2    |

  Scenario: owner above minimum keeps the extra slot
    Given minimum quota 2 with 1 extra partitions
    And members own partitions 3,0,0
    When homogeneous member quotas are assigned
    Then the member quotas are:
      | takes_extra | retain | fill |
      | true        | 3      | 0    |
      | false       | 0      | 2    |
      | false       | 0      | 2    |

  Scenario: no subscribers
    Given subscriber loads:
      | assigned | assigned_at_topic_start |
    When the least loaded subscriber is selected
    Then no subscriber is selected

  Scenario: smallest current load
    Given subscriber loads:
      | assigned | assigned_at_topic_start |
      | 4        | 4                       |
      | 1        | 1                       |
      | 1        | 1                       |
    When the least loaded subscriber is selected
    Then subscriber 1 is selected

  Scenario: first of equal loads
    Given subscriber loads:
      | assigned | assigned_at_topic_start |
      | 2        | 2                       |
      | 2        | 2                       |
    When the least loaded subscriber is selected
    Then subscriber 0 is selected

  Scenario: starting load breaks a level tie
    Given subscriber loads:
      | assigned | assigned_at_topic_start |
      | 1        | 1                       |
      | 1        | 0                       |
    When the least loaded subscriber is selected
    Then subscriber 1 is selected

  Scenario: current load outranks starting load
    Given subscriber loads:
      | assigned | assigned_at_topic_start |
      | 2        | 0                       |
      | 1        | 1                       |
    When the least loaded subscriber is selected
    Then subscriber 1 is selected

  Scenario: maximum loads
    Given subscriber loads:
      | assigned | assigned_at_topic_start |
      | maximum  | 0                       |
      | maximum  | 0                       |
    When the least loaded subscriber is selected
    Then subscriber 0 is selected
