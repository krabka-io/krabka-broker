Feature: Kafka controller listener Admin API surface and operations

  Scenario: Released controller Admin version ranges match Kafka
    Given a combined broker has an open plaintext controller connection with trunk APIs enabled false
    When the client requests ApiVersions from the controller listener
    Then the controller advertises these exact Admin API version ranges and excludes DescribeClientQuotas
      | key | minimum | maximum |
      | 19 | 2 | 7 |
      | 20 | 1 | 6 |
      | 29 | 1 | 3 |
      | 30 | 1 | 3 |
      | 31 | 1 | 3 |
      | 32 | 1 | 4 |
      | 33 | 0 | 2 |
      | 37 | 0 | 3 |
      | 38 | 1 | 3 |
      | 39 | 1 | 2 |
      | 40 | 1 | 2 |
      | 41 | 1 | 3 |
      | 43 | 0 | 2 |
      | 44 | 0 | 1 |
      | 45 | 0 | 1 |
      | 46 | 0 | 0 |
      | 49 | 0 | 1 |
      | 50 | 0 | 0 |
      | 51 | 0 | 0 |
      | 56 | 2 | 3 |
      | 57 | 0 | 2 |
      | 58 | 0 | 0 |
      | 64 | 0 | 0 |
      | 67 | 0 | 0 |
      | 73 | 0 | 0 |
      | 94 | absent | absent |

  Scenario: Released controller listener advertises exactly Kafka controller keys
    Given a combined broker has an open plaintext controller connection with trunk APIs enabled false
    When the client requests ApiVersions from the controller listener
    Then the complete controller API key set is 1,17,18,19,20,29,30,31,32,33,36,37,38,39,40,41,43,44,45,46,49,50,51,52,53,54,55,56,57,58,59,60,62,63,64,67,70,73,80,81,82

  Scenario: Trunk controller Admin version ranges match Kafka
    Given a combined broker has an open plaintext controller connection with trunk APIs enabled true
    When the client requests ApiVersions from the controller listener
    Then the controller advertises these exact Admin API version ranges and excludes DescribeClientQuotas
      | key | minimum | maximum |
      | 19 | 2 | 7 |
      | 20 | 1 | 6 |
      | 29 | 1 | 3 |
      | 30 | 1 | 3 |
      | 31 | 1 | 3 |
      | 32 | 1 | 4 |
      | 33 | 0 | 2 |
      | 37 | 0 | 3 |
      | 38 | 1 | 3 |
      | 39 | 1 | 2 |
      | 40 | 1 | 2 |
      | 41 | 1 | 3 |
      | 43 | 0 | 2 |
      | 44 | 0 | 1 |
      | 45 | 0 | 1 |
      | 46 | 0 | 0 |
      | 49 | 0 | 1 |
      | 50 | 0 | 0 |
      | 51 | 0 | 0 |
      | 56 | 2 | 3 |
      | 57 | 0 | 2 |
      | 58 | 0 | 0 |
      | 64 | 0 | 0 |
      | 67 | 0 | 0 |
      | 73 | 0 | 0 |
      | 94 | 0 | 0 |

  Scenario: Trunk controller listener advertises exactly Kafka controller keys
    Given a combined broker has an open plaintext controller connection with trunk APIs enabled true
    When the client requests ApiVersions from the controller listener
    Then the complete controller API key set is 1,17,18,19,20,29,30,31,32,33,36,37,38,39,40,41,43,44,45,46,49,50,51,52,53,54,55,56,57,58,59,60,62,63,64,67,70,73,80,81,82,94

  Scenario: The controller listener serves the complete topic lifecycle
    Given a combined broker has an open plaintext controller connection with trunk APIs enabled false
    When the controller client creates one partition of controller-lifecycle at replication factor one
    Then the complete create response has a nonzero topic UUID, one partition, replication factor one and nonempty effective configuration
    When the controller client grows controller-lifecycle to three partitions with no manual assignments
    Then the complete CreatePartitions response contains controller-lifecycle with success code zero
    When the controller client deletes controller-lifecycle by name
    Then the complete DeleteTopics response succeeds and carries the UUID minted by CreateTopics

  Scenario: Anonymous plaintext controller token writes are refused
    Given a combined broker has an open plaintext controller connection with trunk APIs enabled false
    When the anonymous plaintext client creates a token with lifetime -1, renews HMAC not-a-token for -1ms and expires it after -1ms
    Then all three complete token responses refuse with code 64, timestamps -1 and anonymous owner and requester

  Scenario: SCRAM credentials written through the controller listener can be described on the same connection
    Given a combined broker has an open plaintext controller connection with trunk APIs enabled false
    When the controller client stores alice's SCRAM-SHA-256 credential at 8192 iterations with salt-bytes and salted-password-bytes
    Then the complete SCRAM alteration response contains only alice with success code zero
    When the same controller connection describes alice's SCRAM credentials
    Then the complete credential description reports alice with SCRAM-SHA-256 and 8192 iterations

  Scenario: Replica directory assignment is served by the controller listener
    Given a combined broker has an open plaintext controller connection with trunk APIs enabled false
    And broker 1 is registered and topic controller-dirs has one partition with its captured topic UUID
    When broker 1 assigns controller-dirs partition 0 to directory UUID 07070707070707070707070707070707 at its registered epoch
    Then the complete directory assignment response echoes directory 07070707070707070707070707070707, topic UUID and partition 0 with success at both levels

  Scenario: Trunk controller unregistration removes registered controllers and names unknown IDs
    Given a combined broker has an open plaintext controller connection with trunk APIs enabled true
    And controller 7 is registered and metadata.version is upgraded to controller unregistration level 33
    When the controller client unregisters controller 7 and then unknown controller 9
    Then the complete unregister responses are success followed by CONTROLLER_ID_NOT_REGISTERED for 9 and controller 7 is absent

  Scenario: Controller-only nodes never host topic replicas
    Given a controller-only node has an open plaintext controller connection and no registered data broker
    When the controller-only client creates controller-only-placement with one partition and replication factor one
    Then the complete create response refuses placement with INVALID_REPLICATION_FACTOR code 38, no topic UUID and empty configs

  Scenario: Broker registration preserves incarnations and validates feature and directory ownership
    Given a combined broker has an open plaintext controller connection with trunk APIs enabled false
    When Kafka brokers send these ordered registrations and heartbeat accepted incarnation a after each registration
      | broker | incarnation | port | directories | metadata.version support | wait milliseconds |
      | 7 | a | 19092 | 7000 | true | 0 |
      | 7 | a | 19093 | 7000 | true | 0 |
      | 7 | b | 19094 | 7000 | true | 0 |
      | 7 | b | 19094 | 7000 | true | 2500 |
      | 8 | 8a | 19100 | 8000 | false | 0 |
      | 8 | 8a | 19100 | none | true | 0 |
      | 8 | 8a | 19100 | 8000,7000 | true | 0 |
    Then the complete registration outcomes match these epoch relations and stored broker states, and both incarnation-a heartbeats succeed
      | code | epoch | port | fenced |
      | 0 | new | 19092 | true |
      | 0 | kept | 19093 | false |
      | 101 | refused | none | none |
      | 0 | new | 19094 | true |
      | 119 | refused | none | none |
      | 119 | refused | none | none |
      | 119 | refused | none | none |

  Scenario: Controller listener serves inter-broker RPCs and refuses SASL after plaintext authentication
    Given a combined broker has an open plaintext controller connection with trunk APIs enabled false
    When broker 1 sends AlterPartition for unknown topic UUID 07070707070707070707070707070707 partition 0, broker 99 allocates producer IDs, and the plaintext connection attempts PLAIN SASL authentication
    Then the full inter-broker responses report UNKNOWN_TOPIC_ID 100 and STALE_BROKER_EPOCH 77, and both SASL responses report ILLEGAL_SASL_STATE 34
