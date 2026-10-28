Feature: AssignReplicasToDirs plans an independent error for every reported partition
  Scenario: Mixed valid missing non-replica and unknown-UUID rows preserve request structure
    Given topic t UUID 66 has replicas 1 and 2 while topic other UUID 67 has only replica 1
    And broker 2 at epoch -1 reports directory UUID 170 for these topic partitions
      | topic_uuid | partitions |
      | 66         | 0,99       |
      | 67         | 0          |
      | 68         | 0,1        |
      | 0          | 0          |
    When the controller plans the complete directory assignment report
    Then only replica 2 of t:0 gets an assignment record and the complete response contains these ordered partition codes
      | topic_uuid | partition_errors |
      | 66         | 0=0,99=3         |
      | 67         | 0=6              |
      | 68         | 0=100,1=100      |
      | 0          | 0=100            |
