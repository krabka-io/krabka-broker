Feature: Added replicas inherit remote history without copying the archive

  Scenario: a replica added to a tiered partition does not pull the archive
    Given three tiered brokers use 1-KiB segments and broker 1 alone hosts the data partition
    When broker 1 acknowledges four hundred 256-byte records
    Then all sealed segments are archived and locally evicted, leaving an archive over four times the retained local bytes
    When the active controller reassigns the partition from replica 1 to replicas 1 and 3
    Then replica 3 catches up, starts at or above the leader's local start, pulls less than half the archive bytes and retains fewer disk bytes than the archive
