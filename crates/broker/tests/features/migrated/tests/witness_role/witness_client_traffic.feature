Feature: Witness replication membership never grants a client-serving role

  Scenario: witness refuses client traffic while replication advances
    Given sites A and B host data brokers 1 and 2, site C hosts witness 3 and all three replicas join witness-topic partition 0 ISR
    When leader 1 receives five records with acks all
    Then the leader succeeds and witness 3 replicates through log end offset 5
    When a client sends Produce and a site-C consumer Fetch directly to witness 3
    Then both requests return NOT_LEADER_OR_FOLLOWER and the complete leader endpoint, Fetch names the current leader, and ISR remains 1,2,3
