Feature: Deleting share offsets removes durable topic state and its initialized metadata

  Scenario: delete removes topic
    Given share group g1 has acquired its three initialized records on t
    When the deletion-test member leaves g1
    And an administrator deletes t's offsets from g1
    Then offset deletion succeeds for the group and topic t
    When the deleted t partition is described until it reports a missing start
    Then the deleted partition's start is minus one without a partition error

  Scenario: delete rewrites metadata topic absent after restart
    Given share group g1 has acquired its three initialized records on t
    When all initialized share offsets of g1 are enumerated
    Then the initialized offset metadata enumerates exactly t partition zero
    When the deletion-test member leaves g1
    And an administrator deletes t's offsets from g1
    Then offset deletion succeeds for the group and topic t
    When offset metadata is polled for up to four seconds until deleted t disappears
    Then the initialized offset metadata contains no t partitions
    When the metadata rewrite is flushed for 300 milliseconds and the share broker restarts
    Then deleted t remains absent from initialized offsets throughout eight seconds of recovery
