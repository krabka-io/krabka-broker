Feature: Local retention rolls an idle active segment without losing the remote range

  Scenario: an idle tiered partition leaves local disk through its last record
    Given a topic-backed tiered partition retains remote records indefinitely and local records for 1000 milliseconds
    When ten timestamp-zero records are appended and the partition receives no more writes
    Then EARLIEST_LOCAL reaches log end 10 while EARLIEST remains 0 and remote log objects exist
