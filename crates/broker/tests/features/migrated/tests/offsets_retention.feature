Feature: Consumer offset retention configuration and dead-group cleanup
  Scenario: Default retention knobs retain Kafka values sources and scalar types
    Given a broker has no explicit offset retention settings
    When all broker configurations are described without synonyms or documentation
    Then the read-only retention entries include
      | name                                | value  | source  | type |
      | offsets.retention.minutes           | 10080  | default | INT  |
      | offsets.retention.check.interval.ms | 600000 | default | LONG |

  Scenario: An explicitly configured default still reports its static broker source
    Given a broker explicitly sets offset retention to Kafka's default 10080 minutes
    When only the two retention configuration keys are described
    Then the read-only retention entries are exactly
      | name                                | value  | source        | type |
      | offsets.retention.check.interval.ms | 600000 | default       | LONG |
      | offsets.retention.minutes           | 10080  | static broker | INT  |

  Scenario: The background sweeper removes a memberless group after its final offset is deleted
    Given memberless group leaked-by-ci has offset five on orders and the broker sweeps every 25 milliseconds
    When the memberless groups are listed
    Then leaked-by-ci appears in the successful group listing
    When the group's last committed offset is deleted
    Then the last offset deletion succeeds
    When the broker's own sweeper is observed for up to ten seconds
    Then the dead group disappears without a DeleteGroups request
