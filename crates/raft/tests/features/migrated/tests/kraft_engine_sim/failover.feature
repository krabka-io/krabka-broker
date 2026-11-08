Feature: Majority failover and former leader recovery

  Scenario: A leader crash is followed by a replacement at a higher epoch
    Given a three voter cluster 300 has elected its initial leader
    When the current leader crashes and the surviving majority elects a replacement
    Then a different surviving voter leads at a higher epoch

  Scenario: A leader crash preserves majority commits
    Given a three voter cluster 300 has elected its initial leader
    And the leader has crashed and the surviving majority has elected a replacement
    When the replacement leader commits the post-failover topic to both survivors
    Then both surviving metadata images contain the post-failover topic

  Scenario: Five leader restarts preserve election convergence
    Given a three voter cluster 301 has elected its initial leader
    When the elected leader is crashed and reopened from its directory five times
    Then all six observed quorums agree on a live leader at an established epoch

  Scenario: An isolated leader resigns
    Given a three voter cluster 302 has elected its initial leader
    When the network isolates the leader until it resigns and the majority elects a replacement
    Then the isolated controller no longer names itself leader
    And a different surviving voter leads at a higher epoch

  Scenario: An isolated leader rejoins after healing
    Given a three voter cluster 302 has elected its initial leader
    And the network has isolated the leader until it resigned and the majority elected a replacement
    When the majority commits post-partition and the network heals the former leader
    Then the former leader follows the replacement epoch and contains post-partition

  Scenario Outline: Lockstep survivors retry split votes on the election timer
    Given cluster <cluster> has uniform 150 millisecond elections and 1500 millisecond Fetch rounds
    When the current leader crashes and the surviving majority elects a replacement
    Then a different surviving voter leads at a higher epoch
    And the replacement election completes in less than 2600 milliseconds

    Examples:
      | case         | cluster |
      | cluster 940  | 940     |
      | cluster 941  | 941     |
      | cluster 942  | 942     |
      | cluster 943  | 943     |
      | cluster 944  | 944     |
      | cluster 945  | 945     |
