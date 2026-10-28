# Quoted <empty> denotes a zero-length Kafka string; an empty created-topic list contains no names.
Feature: Cordoned directories are exposed by new clients and refuse incoming replica moves

  Scenario: Cordoning is versioned in descriptions and blocks moves until removed
    Given a two-log-directory broker hosts topic t with 1 partitions
    And both configured log directories initially report uncordoned
    When broker 1 sets cordoned.log.dirs to "extra configured path" through IncrementalAlterConfigs
    Then the alter succeeds and DescribeLogDirs v5 reports primary uncordoned and extra cordoned true
    And DescribeLogDirs v4 omits cordoning and reports both directories uncordoned
    When AlterReplicaLogDirs moves topic t partitions 0 to the extra directory
    Then the complete replica move response has partition 0 error code 39
    When broker 1 sets cordoned.log.dirs to "<empty>" through IncrementalAlterConfigs
    Then the alter succeeds and DescribeLogDirs v5 reports primary uncordoned and extra cordoned false
    When AlterReplicaLogDirs moves topic t partitions 0 to the extra directory
    Then the complete replica move response has partition 0 error code 0

  Scenario: Cordoning cannot name an unconfigured directory
    Given a two-log-directory broker hosts no requested topic
    When broker 1 sets cordoned.log.dirs to "/nope" through IncrementalAlterConfigs
    Then cordoning refuses with code 42 and message "requirement failed: All entries in cordoned.log.dirs must be present in log.dirs or log.dir. Missing entries : /nope"
