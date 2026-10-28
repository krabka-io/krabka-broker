Feature: ConsumerGroupHeartbeat validates member identity join fields and assignment compatibility
  Scenario Outline: Valid joining and steady heartbeats pass default validation
    Given a consumer <phase> heartbeat uses API version 1
    When the consumer heartbeat is validated with default coordinator settings
    Then consumer heartbeat validation accepts the request
    Examples:
      | phase   |
      | joining |
      | steady  |

  Scenario: Version zero may mint the joining member ID
    Given a consumer joining heartbeat uses API version 0
    And the consumer heartbeat field member ID is empty
    When the consumer heartbeat is validated with default coordinator settings
    Then consumer heartbeat validation accepts the request

  Scenario: Invalid heartbeat fields retain complete Kafka refusal messages: phase=joining, version=1, field=member ID (1)
    Given a consumer joining heartbeat uses API version 1
    And the consumer heartbeat field member ID is blank
    When the consumer heartbeat is validated with default coordinator settings
    Then consumer heartbeat validation refuses with code 42 message MemberId can't be empty.

  Scenario: Invalid heartbeat fields retain complete Kafka refusal messages: phase=leaving, version=0, field=member ID (2)
    Given a consumer leaving heartbeat uses API version 0
    And the consumer heartbeat field member ID is empty
    When the consumer heartbeat is validated with default coordinator settings
    Then consumer heartbeat validation refuses with code 42 message MemberId can't be empty.

  Scenario: Invalid heartbeat fields retain complete Kafka refusal messages: phase=joining, version=1, field=group ID (3)
    Given a consumer joining heartbeat uses API version 1
    And the consumer heartbeat field group ID is empty
    When the consumer heartbeat is validated with default coordinator settings
    Then consumer heartbeat validation refuses with code 42 message GroupId can't be empty.

  Scenario: Invalid heartbeat fields retain complete Kafka refusal messages: phase=joining, version=1, field=instance ID (4)
    Given a consumer joining heartbeat uses API version 1
    And the consumer heartbeat field instance ID is empty
    When the consumer heartbeat is validated with default coordinator settings
    Then consumer heartbeat validation refuses with code 42 message InstanceId can't be empty.

  Scenario: Invalid heartbeat fields retain complete Kafka refusal messages: phase=joining, version=1, field=rack ID (5)
    Given a consumer joining heartbeat uses API version 1
    And the consumer heartbeat field rack ID is empty
    When the consumer heartbeat is validated with default coordinator settings
    Then consumer heartbeat validation refuses with code 42 message RackId can't be empty.

  Scenario: Invalid heartbeat fields retain complete Kafka refusal messages: phase=joining, version=1, field=rebalance timeout (6)
    Given a consumer joining heartbeat uses API version 1
    And the consumer heartbeat field rebalance timeout is -1
    When the consumer heartbeat is validated with default coordinator settings
    Then consumer heartbeat validation refuses with code 42 message RebalanceTimeoutMs must be provided in first request.

  Scenario: Invalid heartbeat fields retain complete Kafka refusal messages: phase=joining, version=1, field=owned partitions (7)
    Given a consumer joining heartbeat uses API version 1
    And the consumer heartbeat field owned partitions is null
    When the consumer heartbeat is validated with default coordinator settings
    Then consumer heartbeat validation refuses with code 42 message TopicPartitions must be empty when (re-)joining.

  Scenario: Invalid heartbeat fields retain complete Kafka refusal messages: phase=joining, version=1, field=owned partitions (8)
    Given a consumer joining heartbeat uses API version 1
    And the consumer heartbeat field owned partitions is nonempty
    When the consumer heartbeat is validated with default coordinator settings
    Then consumer heartbeat validation refuses with code 42 message TopicPartitions must be empty when (re-)joining.

  Scenario: Invalid heartbeat fields retain complete Kafka refusal messages: phase=joining, version=1, field=subscription (9)
    Given a consumer joining heartbeat uses API version 1
    And the consumer heartbeat field subscription is null
    When the consumer heartbeat is validated with default coordinator settings
    Then consumer heartbeat validation refuses with code 42 message Either SubscribedTopicNames or SubscribedTopicRegex must be non-null when (re-)joining.

  Scenario: Invalid heartbeat fields retain complete Kafka refusal messages: phase=steady, version=1, field=member epoch (10)
    Given a consumer steady heartbeat uses API version 1
    And the consumer heartbeat field member epoch is -2
    When the consumer heartbeat is validated with default coordinator settings
    Then consumer heartbeat validation refuses with code 42 message InstanceId can't be null.

  Scenario: Invalid heartbeat fields retain complete Kafka refusal messages: phase=steady, version=1, field=member epoch (11)
    Given a consumer steady heartbeat uses API version 1
    And the consumer heartbeat field member epoch is -3
    When the consumer heartbeat is validated with default coordinator settings
    Then consumer heartbeat validation refuses with code 42 message MemberEpoch is invalid.

  Scenario: Invalid heartbeat fields retain complete Kafka refusal messages: phase=joining, version=1, field=server assignor (12)
    Given a consumer joining heartbeat uses API version 1
    And the consumer heartbeat field server assignor is sticky
    When the consumer heartbeat is validated with default coordinator settings
    Then consumer heartbeat validation refuses with code 112 message ServerAssignor sticky is not supported. Supported assignors: uniform, range.

