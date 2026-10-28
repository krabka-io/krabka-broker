# Quoted <empty> denotes a zero-length Kafka string; an empty created-topic list contains no names.
Feature: Incremental topic configuration merges stored overrides and list defaults

  Scenario: Deleting an unknown key preserves an empty override map
    Given topic orders exists with these stored overrides and the requested topic is "orders"
      | key | value |
    When topic configuration merges operation 1 on not.a.topic.config with value "null"
    Then the whole topic config record contains exactly these overrides
      | key | value |

  Scenario: Append uses the cleanup policy default
    Given topic orders exists with these stored overrides and the requested topic is "orders"
      | key | value |
    When topic configuration merges operation 2 on cleanup.policy with value "compact"
    Then the whole topic config record contains exactly these overrides
      | key | value |
      | cleanup.policy | delete,compact |

  Scenario: Subtract removes only named list elements
    Given topic orders exists with these stored overrides and the requested topic is "orders"
      | key | value |
      | cleanup.policy | compact,delete |
    When topic configuration merges operation 3 on cleanup.policy with value "delete"
    Then the whole topic config record contains exactly these overrides
      | key | value |
      | cleanup.policy | compact |

  Scenario: Throttle replica lists append new assignments
    Given topic orders exists with these stored overrides and the requested topic is "orders"
      | key | value |
      | leader.replication.throttled.replicas | 0:1 |
    When topic configuration merges operation 2 on leader.replication.throttled.replicas with value "1:2"
    Then the whole topic config record contains exactly these overrides
      | key | value |
      | leader.replication.throttled.replicas | 0:1,1:2 |

  Scenario: Appending a repeated throttle replica is idempotent
    Given topic orders exists with these stored overrides and the requested topic is "orders"
      | key | value |
      | leader.replication.throttled.replicas | 0:1 |
    When topic configuration merges operation 2 on leader.replication.throttled.replicas with value "0:1"
    Then the whole topic config record contains exactly these overrides
      | key | value |
      | leader.replication.throttled.replicas | 0:1 |

  Scenario: Append requires a list key
    Given topic orders exists with these stored overrides and the requested topic is "orders"
      | key | value |
    When topic configuration merges operation 2 on retention.ms with value "1"
    Then the topic configuration merge refuses with code 40 and message "Can't APPEND to key retention.ms because its type is not LIST."

  Scenario: Default topic resources are invalid
    Given topic orders exists with these stored overrides and the requested topic is "<empty>"
      | key | value |
    When topic configuration merges operation 0 on retention.ms with value "1000"
    Then the topic configuration merge refuses with code 42 and message "Default topic resources are not allowed."

  Scenario: Name validation precedes topic existence
    Given topic orders exists with these stored overrides and the requested topic is "a/b"
      | key | value |
    When topic configuration merges operation 0 on retention.ms with value "1000"
    Then the topic configuration merge refuses with code 17 and message "Topic name is invalid: 'a/b' contains one or more characters other than ASCII alphanumerics, '.', '_' and '-'"

  Scenario: A valid missing topic reports absence
    Given topic orders exists with these stored overrides and the requested topic is "missing"
      | key | value |
    When topic configuration merges operation 0 on retention.ms with value "1000"
    Then the topic configuration merge refuses with code 3 and message "The topic 'missing' does not exist."
