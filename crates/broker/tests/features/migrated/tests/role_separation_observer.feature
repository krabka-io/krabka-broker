Feature: tests role separation observer

  Scenario: broker only node observes and forwards
    Given controller-only node 1 has 1 broker-only observers
    And the broker heartbeats remain unfenced beyond the seed window
    When CreateTopics sends rolesep-observed through broker-only node 2
    Then the controller commits rolesep-observed, the observer fetches it and node 2 remains outside the voter quorum

  Scenario: a broker only node recovers after the controller prunes past its fetch offset
    Given controller 1 snapshots every four records and retains zero metadata-log bytes while broker-only node 2 keeps default checkpoints
    When the broker-only observer creates these single-partition topics
      | topic |
      | pruned-topic-0 |
      | pruned-topic-1 |
      | pruned-topic-2 |
      | pruned-topic-3 |
      | pruned-topic-4 |
      | pruned-topic-5 |
    Then the controller has a checkpoint replacing pruned metadata records
    When broker-only node 2 restarts on its existing addresses and log directories
    Then the restarted observer restores every topic, re-registers with the controller and reports ready

  Scenario: heartbeats keep brokers unfenced in a role separated cluster
    Given controller-only node 1 has 2 broker-only observers
    And the broker heartbeats remain unfenced beyond the seed window
    Then all broker registrations stay unfenced throughout four additional seconds sampled every 200 milliseconds
    When DescribeCluster reads the default broker listing from both observers
    Then both listings contain exactly unfenced brokers 2 and 3 and name a listed broker as controller
    And Metadata from either observer resolves its controller id to a reachable broker returning the same cluster id

  Scenario: controller only node opens no client listener
    Given controller-only node 1 has 1 broker-only observers
    When a TCP client connects to the controller's configured client address
    Then the connection is refused, the data-plane address is absent and listen_addr names only the controller listener

  Scenario: broker only node forwards describe quorum to controller
    Given controller-only node 1 has 1 broker-only observers
    And the broker heartbeats remain unfenced beyond the seed window
    When DescribeQuorum requests the metadata partition through broker-only node 2
    Then the quorum response succeeds for only __cluster_metadata partition 0, led by voter 1 in an epoch of at least 1

  Scenario: broker only nodes are described as quorum observers
    Given controller-only node 1 has 2 broker-only observers
    When DescribeQuorum is polled for up to thirty seconds until observers 2 and 3 have fetched and caught up
    Then the quorum lists exactly observers 2 and 3 with nonnegative log ends, fetch and caught-up times after 1700000000000, and caught-up times no later than their fetch times

  Scenario: unregister broker sent to a broker only node is decided by the controller
    Given controller-only node 1 alone requires break-glass approvers User:alice and User:bob, with broker-only nodes 2 and 3
    When UnregisterBroker targets node 3 through broker-only node 2
    Then UnregisterBroker returns error code 44
    And the controller retains node 3 and refuses the unapproved request with message
      """
      break-glass refused unregister_broker on 3: no approved proposal covers the request
      """

  Scenario: unregister broker sent to a broker only node reaches the controller
    Given controller-only node 1 has 2 broker-only observers
    When UnregisterBroker targets node 3 through broker-only node 2
    Then UnregisterBroker returns error code 0
    And the controller removes the target broker from its committed image

  Scenario: a separate metadata log directory rolls cleans and restores a wiped node
    Given controller 1 and observer 2 separate data and metadata directories, with 2048-byte snapshots and retention, one-second rolls and 50-millisecond metadata idle records
    When the broker-only observer creates these single-partition topics
      | topic |
      | metadata-dir-topic-0 |
      | metadata-dir-topic-1 |
      | metadata-dir-topic-2 |
    Then the controller cleans its first metadata segment within 100 seconds, data partitions occupy only the data directory and DescribeLogDirs reports only that directory
    When broker-only node 2 stops, both its data and metadata directories are wiped and it restarts on the same addresses
    Then the restarted observer restores every topic, re-registers with the controller and reports ready
