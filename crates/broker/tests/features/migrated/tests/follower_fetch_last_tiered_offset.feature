Feature: A live follower config chooses whether to replicate already-tiered local segments

  Scenario: a new replica starts at the last tiered offset only with the flag
    Given three tiered brokers with one-KiB segments and a thirty-second heartbeat timeout
    And topics kip1023-flag-unset,kip1023-cluster-default,kip1023-new-replica-only on broker one each archive 60 records of 256 bytes with local retention 4096 bytes
    When broker three joins topic kip1023-flag-unset with last-tiered-offset config scope unset
    Then the leader retains sealed tiered bytes below its pending upload offset
    And the new replica pulls at least the leader's sealed local byte count
    When broker three joins topic kip1023-cluster-default with last-tiered-offset config scope cluster-default
    Then the leader retains sealed tiered bytes below its pending upload offset
    And the new replica starts at or above pending upload and pulls fewer bytes than the leader's sealed local byte count
    When broker three joins topic kip1023-new-replica-only with last-tiered-offset config scope broker-three-only
    Then the leader retains sealed tiered bytes below its pending upload offset
    And the new replica starts at or above pending upload and pulls fewer bytes than the leader's sealed local byte count
