Feature: Majority failover and former leader recovery
  Scenario: A leader crash preserves majority commits
    Given a three voter cluster 300 has elected its initial leader
    When the current leader crashes and the surviving majority elects a replacement
    Then a different surviving voter leads at a higher epoch
    When the replacement leader commits the post-failover topic to both survivors
    Then both surviving metadata images contain the post-failover topic
  Scenario: Five leader restarts preserve election convergence
    Given a three voter cluster 301 has elected its initial leader
    When the elected leader is crashed and reopened from its directory five times
    Then all six observed quorums agree on a live leader at an established epoch
  Scenario: An isolated leader resigns and rejoins after healing
    Given a three voter cluster 302 has elected its initial leader
    When the network isolates the leader until it resigns and the majority elects a replacement
    Then the isolated controller no longer names itself leader
    And a different surviving voter leads at a higher epoch
    When the majority commits post-partition and the network heals the former leader
    Then the former leader follows the replacement epoch and contains post-partition
  Scenario Outline: Lockstep survivors retry split votes on the election timer
    Given cluster <cluster> has uniform 150 millisecond elections and 1500 millisecond Fetch rounds
    When the current leader crashes and the surviving majority elects a replacement
    Then a different surviving voter leads at a higher epoch
    And the replacement election completes in less than 2600 milliseconds
    Examples:
      | cluster |
      | 940 |
      | 941 |
      | 942 |
      | 943 |
      | 944 |
      | 945 |
