Feature: Migration diagnostics interpret stock Kafka and Docker readings
  Scenario: Rendered source records retain their contents when parsed
    Given the migration fixture's keyed and headered records
    When the seeded records are rendered and parsed as console lines
    Then every rendered record retains the original headers key and value

  Scenario: Notices and malformed lines are not mistaken for records
    Given the console formatter outputs these lines
      | line |
      | Processed a total of 10 messages |
      | |
      | a	b	c	d |
    When the console record parser interprets those lines
    Then all notices and malformed lines remain absent records

  Scenario: Headerless console records preserve their explicit header marker
    Given the console formatter outputs these lines
      | line |
      | NO_HEADERS	key-0	order-0 |
    When the console record parser interprets those lines
    Then the decoded record has the expected headers key and value
      | headers | key | value |
      | NO_HEADERS | key-0 | order-0 |

  Scenario: Only MirrorMaker internal topic shapes receive migration roles
    Given the target has topic names
      | topic |
      | heartbeats |
      | source.checkpoints.internal |
      | mm2-offset-syncs.target.internal |
      | mm2-offset-syncs.source.internal |
      | mm2-offsets.source.internal |
      | mm2-configs.source.internal |
      | mm2-status.source.internal |
      | source.krabka-mm2-orders |
      | __consumer_offsets |
    When the migration inspector classifies the internal topic roles
    Then only heartbeat checkpoint and offset sync topic shapes have these roles
      | role |
      | Heartbeats |
      | Checkpoints |
      | OffsetSyncs |
      | OffsetSyncs |
      | none |
      | none |
      | none |
      | none |
      | none |

  Scenario: A topic report exposes its partition count without inventing missing values
    Given stock Kafka prints the migration inspection report
      """
      Topic: heartbeats	TopicId: sVCK0oNBQtOb4tGXEmuJHw	PartitionCount: 1	ReplicationFactor: 1	Configs: cleanup.policy=compact
      	Topic: heartbeats	Partition: 0	Leader: 1	Replicas: 1	Isr: 1
      """
    When the inspector reads the partition count
    Then the report describes one partition
    Given stock Kafka prints the migration inspection report
      """
      Topic: heartbeats
      """
    When the inspector reads the partition count
    Then the missing partition count remains absent

  Scenario: Effective configuration values are read outside repeated synonym fields
    Given stock Kafka prints the migration inspection report
      """
      Dynamic configs for topic heartbeats are:
        cleanup.policy=compact sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:cleanup.policy=compact}
      """
    When the inspector reads cleanup policy and an absent retention setting
    Then the effective cleanup policy is compact and no retention setting is invented

  Scenario: Committed offsets are selected by both group and topic
    Given stock Kafka prints the migration inspection report
      """
      GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
      krabka-mm2-riders source.krabka-mm2-orders 0 4 10 6 - - -
      krabka-mm2-riders source.other 0 9 9 0 - - -
      """
    When the inspector reads the rider group's mirrored orders offset and an absent topic
    Then the orders offset is four and the absent topic has no offset

  Scenario: An uncommitted group is not treated as committed at zero
    Given stock Kafka prints the migration inspection report
      """
      krabka-mm2-riders source.krabka-mm2-orders 0 - 10 - - - -
      """
    When the inspector reads the rider group's uncommitted orders offset
    Then an uncommitted group has no offset rather than zero

  Scenario: Shutdown noise does not displace the original failure cause
    Given a herder error is followed by five hundred shutdown notices and the final stopped notice
    When the migration diagnostic excerpts the mirror log
    Then the bounded excerpt retains both the original herder cause and the final shutdown notice

  Scenario: Warning only logs remain bounded and retain recent causes
    Given the mirror log contains five hundred warning lines
    When the migration diagnostic excerpts the mirror log
    Then the bounded excerpt retains the newest warning and omits the first warning

  Scenario: Empty logs render both empty diagnostic sections
    Given the mirror log is empty
    When the migration diagnostic excerpts the mirror log
    Then the empty excerpt reports zero lines problems and section lengths

  Scenario: Container inspection preserves running stopped and absent states
    Given Docker inspect prints these container or network outputs
      | stdout |
      | true\|0\n |
      | false\|143\n |
      | |
    When the inspector parses running zero stopped 143 and missing container renderings
    Then the interpreted container states are
      | running | exit_code |
      | true | 0 |
      | false | 143 |
      | missing | missing |

  Scenario: Docker network inspection uses the subnet when the explicit gateway is absent
    Given Docker inspect prints these container or network outputs
      | stdout |
      | 172.17.0.1\|172.17.0.0/16\n |
      | \|172.20.0.0/16\n |
      | no separator here |
    When the inspector reads explicit subnet fallback and malformed network metadata
    Then the interpreted bridge gateways are
      | gateway |
      | 172.17.0.1 |
      | 172.20.0.1 |
      | missing |
