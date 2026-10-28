Feature: Witness replication membership never grants a client-serving role

  Scenario: witness is a visible isr member that serves no reads
    Given sites A and B host data brokers 1 and 2, site C hosts witness 3 and all three replicas join witness-topic partition 0 ISR
    When Metadata lists brokers and describes witness-topic partition 0
    Then Metadata lists racks for brokers 1, 2 and witness 3 and the partition is led by 1 with replicas and ISR 1,2,3 and no offline replica
    When a consumer in site C fetches from leader 1
    Then the consumer receives preferred_read_replica -1 rather than a witness redirect
