Feature: Advertised controller identity follows replicated broker liveness

  Scenario: Follower discovery excludes a crashed broker after replicated fencing
    Given three registered live brokers rotate all advertised controller identities through a follower observer
    When one other follower crashes, its fence is replicated and the observer serves six Metadata and DescribeCluster request pairs
    Then every response advertises a surviving controller, Metadata excludes the dead broker and the observer remains a follower

  Scenario: A restarted ephemeral controller port keeps the broker heartbeat alive
    Given a for-tests broker survives three heartbeat timeouts and a fencing publication on its ephemeral controller port
    When the broker rejoins from the same log directory while its first controller port is held unavailable
    Then both initial and rejoined brokers have a live heartbeat session and no fenced brokers
