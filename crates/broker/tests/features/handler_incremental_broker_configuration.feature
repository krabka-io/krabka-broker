# Quoted <empty> denotes a zero-length Kafka string; an empty created-topic list contains no names.
Feature: Incremental broker configuration and cordoned log directories

  Scenario: leader.replication.throttled.rate operation 0 at broker 1 with value 2048
    Given broker 1 serves resource "1" with ELR enabled false at metadata.version 30
    When IncrementalAlterConfigs applies operation 0 to key leader.replication.throttled.rate with value "2048"
    Then the staged broker config is node 1, key leader.replication.throttled.rate and value "2048"

  Scenario: leader.replication.throttled.rate operation 0 at broker 2 with value 2048
    Given broker 1 serves resource "2" with ELR enabled false at metadata.version 30
    When IncrementalAlterConfigs applies operation 0 to key leader.replication.throttled.rate with value "2048"
    Then the incremental broker config is refused with code 42 and message "Unexpected broker id, expected 1, but received 2"

  Scenario: leader.replication.throttled.rate operation 0 at broker x with value 2048
    Given broker 1 serves resource "x" with ELR enabled false at metadata.version 30
    When IncrementalAlterConfigs applies operation 0 to key leader.replication.throttled.rate with value "2048"
    Then the incremental broker config is refused with code 42 and message "Node id must be an integer, but it is: x"

  Scenario: leader.replication.throttled.rate operation 7 at broker 1 with value 1
    Given broker 1 serves resource "1" with ELR enabled false at metadata.version 30
    When IncrementalAlterConfigs applies operation 7 to key leader.replication.throttled.rate with value "1"
    Then the incremental broker config is refused with code 42 and message "Unknown operations type 7"

  Scenario: log.retention.ms operation 0 at broker default with value 86400000
    Given broker 1 serves resource "<empty>" with ELR enabled false at metadata.version 30
    When IncrementalAlterConfigs applies operation 0 to key log.retention.ms with value "86400000"
    Then the staged broker config is node -1, key log.retention.ms and value "86400000"

  Scenario: num.io.threads operation 0 at broker default with value 16
    Given broker 1 serves resource "<empty>" with ELR enabled false at metadata.version 30
    When IncrementalAlterConfigs applies operation 0 to key num.io.threads with value "16"
    Then the staged broker config is node -1, key num.io.threads and value "16"

  Scenario: listener.name.client.ssl.keystore.location operation 0 at broker 1 with value /k
    Given broker 1 serves resource "1" with ELR enabled false at metadata.version 30
    When IncrementalAlterConfigs applies operation 0 to key listener.name.client.ssl.keystore.location with value "/k"
    Then the staged broker config is node 1, key listener.name.client.ssl.keystore.location and value "/k"

  Scenario: log.dirs operation 0 at broker default with value /tmp
    Given broker 1 serves resource "<empty>" with ELR enabled false at metadata.version 30
    When IncrementalAlterConfigs applies operation 0 to key log.dirs with value "/tmp"
    Then the incremental broker config is refused with code 42 and message "Cannot update these configs dynamically: [log.dirs]"

  Scenario: auto.leader.rebalance.enable operation 0 at broker default with value false
    Given broker 1 serves resource "<empty>" with ELR enabled false at metadata.version 30
    When IncrementalAlterConfigs applies operation 0 to key auto.leader.rebalance.enable with value "false"
    Then the incremental broker config is refused with code 42 and message "Cannot update these configs dynamically: [auto.leader.rebalance.enable]"

  Scenario: max.connections operation 0 at broker default with value -5
    Given broker 1 serves resource "<empty>" with ELR enabled false at metadata.version 30
    When IncrementalAlterConfigs applies operation 0 to key max.connections with value "-5"
    Then the incremental broker config is refused with code 42 and message "Invalid value -5 for configuration max.connections: Value must be at least 0"

  Scenario: metric.reporters operation 2 at broker default with value com.example.Reporter
    Given broker 1 serves resource "<empty>" with ELR enabled false at metadata.version 30
    When IncrementalAlterConfigs applies operation 2 to key metric.reporters with value "com.example.Reporter"
    Then the staged broker config is node -1, key metric.reporters and value "org.apache.kafka.common.metrics.JmxReporter,com.example.Reporter"

  Scenario: leader.replication.throttled.rate operation 0 at broker 1 with value -5
    Given broker 1 serves resource "1" with ELR enabled false at metadata.version 30
    When IncrementalAlterConfigs applies operation 0 to key leader.replication.throttled.rate with value "-5"
    Then the incremental broker config is refused with code 42 and message "Invalid value -5 for configuration leader.replication.throttled.rate: Value must be at least 0"

  Scenario: unclean.leader.election.enable operation 0 at broker 1 with value true
    Given broker 1 serves resource "1" with ELR enabled false at metadata.version 30
    When IncrementalAlterConfigs applies operation 0 to key unclean.leader.election.enable with value "true"
    Then the staged broker config is node 1, key unclean.leader.election.enable and value "true"

  Scenario: min.insync.replicas operation 0 at broker 1 with value 2
    Given broker 1 serves resource "1" with ELR enabled false at metadata.version 30
    When IncrementalAlterConfigs applies operation 0 to key min.insync.replicas with value "2"
    Then the staged broker config is node 1, key min.insync.replicas and value "2"

  Scenario: min.insync.replicas operation 0 at broker 1 with value 2 while ELR is enabled
    Given broker 1 serves resource "1" with ELR enabled true at metadata.version 30
    When IncrementalAlterConfigs applies operation 0 to key min.insync.replicas with value "2"
    Then the incremental broker config is refused with code 40 and message "Broker-level min.insync.replicas cannot be altered while ELR is enabled."

  Scenario: min.insync.replicas operation 1 at broker default with value null while ELR is enabled
    Given broker 1 serves resource "<empty>" with ELR enabled true at metadata.version 30
    When IncrementalAlterConfigs applies operation 1 to key min.insync.replicas with value "null"
    Then the incremental broker config is refused with code 40 and message "Cluster-level min.insync.replicas cannot be removed while ELR is enabled."

  Scenario: leader.replication.throttled.rate operation 2 at broker 1 with value 1
    Given broker 1 serves resource "1" with ELR enabled false at metadata.version 30
    When IncrementalAlterConfigs applies operation 2 to key leader.replication.throttled.rate with value "1"
    Then the incremental broker config is refused with code 40 and message "Config value append is not allowed for config key: leader.replication.throttled.rate"

  Scenario: not.a.kafka.key operation 3 at broker 1 with value 1
    Given broker 1 serves resource "1" with ELR enabled false at metadata.version 30
    When IncrementalAlterConfigs applies operation 3 to key not.a.kafka.key with value "1"
    Then the incremental broker config is refused with code 40 and message "Unknown config name: not.a.kafka.key"

  Scenario: log.cleanup.policy operation 2 at broker default with value compact
    Given broker 1 serves resource "<empty>" with ELR enabled false at metadata.version 30
    When IncrementalAlterConfigs applies operation 2 to key log.cleanup.policy with value "compact"
    Then the staged broker config is node -1, key log.cleanup.policy and value "delete,compact"

  Scenario: log.cleanup.policy operation 3 at broker default with value delete
    Given broker 1 serves resource "<empty>" with ELR enabled false at metadata.version 30
    And broker configuration already stores node -1, key log.cleanup.policy and value "compact,delete"
    When IncrementalAlterConfigs applies operation 3 to key log.cleanup.policy with value "delete"
    Then the staged broker config is node -1, key log.cleanup.policy and value "compact"

  Scenario: cordoned.log.dirs operation 0 at broker 1 with value /tmp/d1
    Given broker 1 serves resource "1" with ELR enabled false at metadata.version 30
    And the broker has configured log directories /tmp/d1 and /tmp/d2
    When IncrementalAlterConfigs applies operation 0 to key cordoned.log.dirs with value "/tmp/d1"
    Then the staged broker config is node 1, key cordoned.log.dirs and value "/tmp/d1"

  Scenario: cordoned.log.dirs operation 0 at broker 1 with value *
    Given broker 1 serves resource "1" with ELR enabled false at metadata.version 30
    And the broker has configured log directories /tmp/d1 and /tmp/d2
    When IncrementalAlterConfigs applies operation 0 to key cordoned.log.dirs with value "*"
    Then the staged broker config is node 1, key cordoned.log.dirs and value "*"

  Scenario: cordoned.log.dirs operation 2 at broker 1 with value /tmp/d2
    Given broker 1 serves resource "1" with ELR enabled false at metadata.version 30
    And the broker has configured log directories /tmp/d1 and /tmp/d2
    And broker configuration already stores node 1, key cordoned.log.dirs and value "/tmp/d1"
    When IncrementalAlterConfigs applies operation 2 to key cordoned.log.dirs with value "/tmp/d2"
    Then the staged broker config is node 1, key cordoned.log.dirs and value "/tmp/d1,/tmp/d2"

  Scenario: cordoned.log.dirs operation 3 at broker 1 with value /tmp/d1
    Given broker 1 serves resource "1" with ELR enabled false at metadata.version 30
    And the broker has configured log directories /tmp/d1 and /tmp/d2
    And broker configuration already stores node 1, key cordoned.log.dirs and value "/tmp/d1,/tmp/d2"
    When IncrementalAlterConfigs applies operation 3 to key cordoned.log.dirs with value "/tmp/d1"
    Then the staged broker config is node 1, key cordoned.log.dirs and value "/tmp/d2"

  Scenario: cordoned.log.dirs operation 0 at broker 1 with value /tmp/nope
    Given broker 1 serves resource "1" with ELR enabled false at metadata.version 30
    And the broker has configured log directories /tmp/d1 and /tmp/d2
    When IncrementalAlterConfigs applies operation 0 to key cordoned.log.dirs with value "/tmp/nope"
    Then the incremental broker config is refused with code 42 and message "requirement failed: All entries in cordoned.log.dirs must be present in log.dirs or log.dir. Missing entries : /tmp/nope"

  Scenario: cordoned.log.dirs operation 0 at broker 1 with value *,/tmp/d1
    Given broker 1 serves resource "1" with ELR enabled false at metadata.version 30
    And the broker has configured log directories /tmp/d1 and /tmp/d2
    When IncrementalAlterConfigs applies operation 0 to key cordoned.log.dirs with value "*,/tmp/d1"
    Then the incremental broker config is refused with code 42 and message "requirement failed: When cordoned.log.dirs is set to *, it must not contain other values"

  Scenario: cordoned.log.dirs operation 0 at broker 1 with value /tmp/d1,
    Given broker 1 serves resource "1" with ELR enabled false at metadata.version 30
    And the broker has configured log directories /tmp/d1 and /tmp/d2
    When IncrementalAlterConfigs applies operation 0 to key cordoned.log.dirs with value "/tmp/d1,"
    Then the incremental broker config is refused with code 42 and message "Configuration 'cordoned.log.dirs' values must not be empty."

  Scenario: cordoned.log.dirs operation 0 at broker default with value /tmp/d1
    Given broker 1 serves resource "<empty>" with ELR enabled false at metadata.version 30
    And the broker has configured log directories /tmp/d1 and /tmp/d2
    When IncrementalAlterConfigs applies operation 0 to key cordoned.log.dirs with value "/tmp/d1"
    Then the incremental broker config is refused with code 42 and message "Cannot update these configs at default cluster level, broker id must be specified: [cordoned.log.dirs]"

  Scenario: cordoned.log.dirs operation 0 at broker 1 with value empty before directory cordoning is supported
    Given broker 1 serves resource "1" with ELR enabled false at metadata.version 29
    And the broker has configured log directories /tmp/d1 and /tmp/d2
    When IncrementalAlterConfigs applies operation 0 to key cordoned.log.dirs with value "<empty>"
    Then the incremental broker config is refused with code 40 and message "The cordoned.log.dirs configuration value cannot be set because it requires metadata.version >= 4.3-IV0"

  Scenario: cordoned.log.dirs operation 1 at broker 1 with value null before directory cordoning is supported
    Given broker 1 serves resource "1" with ELR enabled false at metadata.version 29
    And the broker has configured log directories /tmp/d1 and /tmp/d2
    When IncrementalAlterConfigs applies operation 1 to key cordoned.log.dirs with value "null"
    Then the incremental broker config is refused with code 40 and message "The cordoned.log.dirs configuration value cannot be set because it requires metadata.version >= 4.3-IV0"
