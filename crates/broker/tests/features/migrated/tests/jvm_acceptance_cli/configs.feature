@docker
Feature: Operator configuration changes through Kafka command-line tools
  Scenario: A topic configuration alteration is visible in its description
    Given administrative topic krabka-cfg-alter-itest exists with one partition
    And the requested topic configuration settings are
      | key          | value |
      | retention.ms | 60000 |
    When kafka-configs applies the requested topic settings
    And kafka-configs describes the topic overrides
    Then every requested topic setting is described unchanged

  Scenario: Effective topic values retain their dynamic and inherited provenance
    Given administrative topic krabka-cfg-describe-all-itest exists with one partition
    And the requested topic configuration settings are
      | key          | value |
      | retention.ms | 60000 |
    When kafka-configs applies the requested topic settings
    And kafka-configs sets the cluster default unclean.leader.election.enable to true
    And kafka-configs describes all effective topic values
    Then the effective description includes these value and provenance chains
      | rendering |
      | retention.ms=60000 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=60000, DEFAULT_CONFIG:log.retention.hours=168} |
      | unclean.leader.election.enable=true sensitive=false synonyms={DYNAMIC_DEFAULT_BROKER_CONFIG:unclean.leader.election.enable=true, DEFAULT_CONFIG:unclean.leader.election.enable=false} |
      | cleanup.policy=delete sensitive=false synonyms={DEFAULT_CONFIG:log.cleanup.policy=delete} |

  Scenario: All registered topic configuration keys round trip through the stock tool
    Given administrative topic krabka-cfg-every-key-itest exists with one partition
    And the requested topic configuration settings are
      | key                             | value      |
      | segment.ms                      | 60000      |
      | segment.index.bytes             | 10485760   |
      | segment.jitter.ms                | 0          |
      | min.compaction.lag.ms           | 0          |
      | max.compaction.lag.ms           | 86400000   |
      | min.cleanable.dirty.ratio        | 0.25       |
      | file.delete.delay.ms            | 60000      |
      | flush.messages                  | 10000      |
      | flush.ms                        | 1000       |
      | index.interval.bytes            | 8192       |
      | preallocate                     | false      |
      | message.timestamp.type          | CreateTime |
      | message.timestamp.after.max.ms  | 3600000    |
      | message.timestamp.before.max.ms | 3600000    |
    When kafka-configs applies the requested topic settings
    And kafka-configs describes the topic overrides
    Then every requested topic setting is described unchanged

  Scenario: Kafka Streams internal topic configurations are accepted unchanged
    Given Kafka Streams requires these internal topic configurations
      | topic                                    | key                    | value          |
      | krabka-streams-repartition-itest          | cleanup.policy         | delete         |
      | krabka-streams-repartition-itest          | segment.bytes          | 52428800       |
      | krabka-streams-repartition-itest          | retention.ms           | -1             |
      | krabka-streams-repartition-itest          | message.timestamp.type | CreateTime     |
      | krabka-streams-windowed-changelog-itest   | cleanup.policy         | compact,delete |
      | krabka-streams-windowed-changelog-itest   | retention.ms           | 86400000       |
      | krabka-streams-windowed-changelog-itest   | min.compaction.lag.ms   | 0              |
      | krabka-streams-windowed-changelog-itest   | message.timestamp.type | CreateTime     |
    When kafka-topics creates and kafka-configs describes those Streams topics
    Then the Streams topic cleanup policies are preserved
      | topic                                   | policy         |
      | krabka-streams-repartition-itest         | delete         |
      | krabka-streams-windowed-changelog-itest  | compact,delete |

  Scenario: A compacted topic cannot enable tiered storage
    Given compacted topic krabka-cfg-compact-tiered-itest uses compact and delete cleanup
    When kafka-configs enables remote storage on the compacted topic
    Then tiered storage is refused with Kafka's configuration validation explanation

  Scenario: Configuration entity round trips match stock Kafka across both admin APIs
    Given stock Kafka and krabka receive these configuration entity changes
      | entity              | selector                                                                                                                    | settings                                               | keys                                 | quota_entity                                                      |
      | clients             | --entity-type clients --entity-name krabka-client                                                                           | consumer_byte_rate=1024,producer_byte_rate=2048         | consumer_byte_rate,producer_byte_rate | client-id 'krabka-client'                                          |
      | groups              | --entity-type groups --entity-name krabka-cfg-grp                                                                           | share.auto.offset.reset=earliest                       | share.auto.offset.reset              | none                                                              |
      | client-metrics      | --entity-type client-metrics --entity-name krabka-cfg-metrics                                                               | interval.ms=60000,metrics=org.apache.kafka.             | interval.ms,metrics                  | none                                                              |
      | users+clients tuple | --entity-type users --entity-name krabka-cfg-user --entity-type clients --entity-name krabka-client                        | request_percentage=42                                 | request_percentage                   | user-principal 'krabka-cfg-user', client-id 'krabka-client'          |
    When the same Kafka tool alters and describes every entity on both brokers
    Then both tools accept every change and return all requested keys and complete entity identities
    And krabka and stock Kafka agree on every described configuration value
