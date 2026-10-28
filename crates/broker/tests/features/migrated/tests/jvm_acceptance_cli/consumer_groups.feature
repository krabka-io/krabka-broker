Feature: Kafka consumer group administration
  Scenario: kafka consumer groups list describe
    Given console group krabka-cg-list-grp consumes alpha from topic krabka-cg-list-itest with 1 partitions and closes
    When the group tool lists groups and describes the console group
    Then the listing contains the group and its description contains the topic

  Scenario: kafka consumer groups delete offsets
    Given console group krabka-cg-delete-offsets-grp consumes alpha from topic krabka-cg-delete-offsets-itest with 2 partitions and closes
    When the administrator describes the console group's committed offsets
    Then the group's committed offset description contains its topic
    When the Kafka 2.7 tool deletes that group's topic offsets with a piped yes confirmation
    Then offset deletion succeeds, identifies success or the topic, and no group-topic row remains

  Scenario: reset offsets modes match apache kafka
    Given Kafka and Krabka have six records on krabka-reset-offsets-itest and these groups parked at offset 3
      | group                    |
      | krabka-reset-dry-grp      |
      | krabka-reset-execute-grp  |
      | krabka-reset-file-grp     |
    When both group tools dry-run these reset arguments in order
      | arguments                                |
      | --to-earliest                            |
      | --to-latest                              |
      | --to-current                             |
      | --to-offset 2                            |
      | --shift-by -1                            |
      | --shift-by 2                             |
      | --shift-by 100                           |
      | --to-datetime 1970-01-01T00:00:00.000      |
      | --to-datetime 2999-01-01T00:00:00.000      |
      | --by-duration P0DT0H0M0S                  |
      | --by-duration P3650DT0H0M0S               |
    Then both tools propose these offsets in order and every dry run leaves committed offset 3 unchanged
      | offset |
      | 0      |
      | 6      |
      | 3      |
      | 2      |
      | 2      |
      | 5      |
      | 6      |
      | 0      |
      | 6      |
      | 6      |
      | 0      |
    When both tools execute to-earliest for the execute group
    Then both execution tables and committed offsets become 0
    When both tools export the file group's current offsets
    Then both exports contain one matching topic row at offset 3
    When both file groups move to earliest and execute from their exported CSV
    Then both from-file result tables and committed offsets return to 3

  Scenario: group delete and reset refusals match apache kafka
    Given Kafka and Krabka have six records, empty group krabka-group-empty-grp, absent group krabka-group-absent-grp, and a future live group krabka-group-live-grp
    When a detached consumer joins each live group and both tools try to reset then delete it
    Then live resets propose no offsets and explain can only be reset if the group while deletions report org.apache.kafka.common.errors.GroupNotEmptyException
    When both tools delete the absent and empty groups then list remaining groups
    Then absent groups report org.apache.kafka.common.errors.GroupIdNotFoundException while empty groups delete successfully and disappear from successful listings
