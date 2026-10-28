Feature: Streams heartbeat request contracts and validation precedence

  Scenario: a valid join
    Given a streams heartbeat at member epoch 0
    When Kafka trunk streams heartbeat validation runs
    Then the streams request is accepted

  Scenario: a valid heartbeat
    Given a streams heartbeat at member epoch 3
    When Kafka trunk streams heartbeat validation runs
    Then the streams request is accepted

  Scenario: an empty member id
    Given a streams heartbeat at member epoch 0
    And the streams request contains these fields
      | field | value |
      | member_id |  |
    When Kafka trunk streams heartbeat validation runs
    Then the streams request is refused with code 42 and message "MemberId can't be empty."

  Scenario: a blank group id
    Given a streams heartbeat at member epoch 0
    And the streams request contains these fields
      | field | value |
      | group_id | space-tab |
    When Kafka trunk streams heartbeat validation runs
    Then the streams request is refused with code 42 and message "GroupId can't be empty."

  Scenario: an empty instance id
    Given a streams heartbeat at member epoch 0
    And the streams request contains these fields
      | field | value |
      | instance_id |  |
    When Kafka trunk streams heartbeat validation runs
    Then the streams request is refused with code 42 and message "InstanceId can't be empty."

  Scenario: an empty rack id
    Given a streams heartbeat at member epoch 0
    And the streams request contains these fields
      | field | value |
      | rack_id |  |
    When Kafka trunk streams heartbeat validation runs
    Then the streams request is refused with code 42 and message "RackId can't be empty."

  Scenario: a join without a rebalance timeout
    Given a streams heartbeat at member epoch 0
    And the streams request contains these fields
      | field | value |
      | rebalance_timeout_ms | -1 |
    When Kafka trunk streams heartbeat validation runs
    Then the streams request is refused with code 42 and message "RebalanceTimeoutMs must be provided in first request."

  Scenario: a join with active tasks
    Given a streams heartbeat at member epoch 0
    And the streams request contains these fields
      | field | value |
      | active_tasks | subtopology 0 partition 0 |
    When Kafka trunk streams heartbeat validation runs
    Then the streams request is refused with code 42 and message "ActiveTasks must be empty when (re-)joining."

  Scenario: a join with a null active task list
    Given a streams heartbeat at member epoch 0
    And the streams request contains these fields
      | field | value |
      | active_tasks | null |
    When Kafka trunk streams heartbeat validation runs
    Then the streams request is refused with code 42 and message "ActiveTasks must be empty when (re-)joining."

  Scenario: a join with standby tasks
    Given a streams heartbeat at member epoch 0
    And the streams request contains these fields
      | field | value |
      | standby_tasks | subtopology 0 partition 0 |
    When Kafka trunk streams heartbeat validation runs
    Then the streams request is refused with code 42 and message "StandbyTasks must be empty when (re-)joining."

  Scenario: a join with warmup tasks
    Given a streams heartbeat at member epoch 0
    And the streams request contains these fields
      | field | value |
      | warmup_tasks | subtopology 0 partition 0 |
    When Kafka trunk streams heartbeat validation runs
    Then the streams request is refused with code 42 and message "WarmupTasks must be empty when (re-)joining."

  Scenario: a join without a topology
    Given a streams heartbeat at member epoch 0
    And the streams request contains these fields
      | field | value |
      | topology | null |
    When Kafka trunk streams heartbeat validation runs
    Then the streams request is refused with code 42 and message "Topology must be non-null when (re-)joining."

  Scenario: a changelog topic with a partition count
    Given a streams heartbeat at member epoch 0
    And the streams request contains these fields
      | field | value |
      | changelog_partitions | 2 |
    When Kafka trunk streams heartbeat validation runs
    Then the streams request is refused with code 130 and message "Changelog topic store-changelog must have an undefined partition count, but it is set to 2."

  Scenario: a source topic regex precedes the empty member check
    Given a streams heartbeat at member epoch 0
    And the streams request contains these fields
      | field | value |
      | member_id |  |
      | source_topic_regex | in-.* |
    When Kafka trunk streams heartbeat validation runs
    Then the streams request is refused with code 42 and message "Regular expressions for source topics are not supported yet."

  Scenario: epoch minus three
    Given a streams heartbeat at member epoch -3
    When Kafka trunk streams heartbeat validation runs
    Then the streams request is refused with code 42 and message "MemberEpoch is -3, but must be greater than or equal to -2."

  Scenario: static leave without an instance id
    Given a streams heartbeat at member epoch -2
    When Kafka trunk streams heartbeat validation runs
    Then the streams request is refused with code 42 and message "InstanceId can't be null."

  Scenario: one task list without the others
    Given a streams heartbeat at member epoch 3
    And the streams request contains these fields
      | field | value |
      | active_tasks | empty |
    When Kafka trunk streams heartbeat validation runs
    Then the streams request is refused with code 42 and message "If one task-type is non-null, all must be non-null."

  Scenario: a topology on a heartbeat
    Given a streams heartbeat at member epoch 3
    And the streams request contains these fields
      | field | value |
      | topology | epoch 1 source in |
    When Kafka trunk streams heartbeat validation runs
    Then the streams request is refused with code 42 and message "Topology can only be provided when (re-)joining."
