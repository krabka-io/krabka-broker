Feature: Replica reassignment materializes preferences, exposes progress and completes or cancels correctly

  Scenario: preplacement log dir preference survives until reassignment materializes the replica
    Given three plaintext brokers each have an extra log directory and RF2 preplaced partition 0 leaves one broker unassigned
    When AlterReplicaLogDirs prefers the unassigned broker's extra directory for preplaced partition 0
    Then the preference returns REPLICA_NOT_AVAILABLE code 9 before the replica exists
    When AlterPartitionReassignments targets the staying and new replicas through the active controller
    Then the partition reassignment response is exactly partition 0 error 0
    And the new replica materializes as a non-future partition in its preferred extra directory within sixty seconds

  Scenario: alter then complete via isr catchup
    Given three plaintext brokers host RF2 foo partition 0 and the replacement keeps the first replica while adding the unassigned broker
    When AlterPartitionReassignments targets the staying and new replicas through the active controller
    Then the partition reassignment response is exactly partition 0 error 0
    And the assignment is in flight with the new broker adding and the original last broker removing
    When the ISR includes the staying, new and removing replicas
    Then completion clears adding and removing replicas and leaves exactly the target replica set

  Scenario: list in flight returns pending rows
    Given three plaintext brokers host RF2 foo partition 0 and the replacement keeps the first replica while adding the unassigned broker
    When AlterPartitionReassignments targets the staying and new replicas through the active controller
    Then the assignment is in flight with the new broker adding and the original last broker removing
    When ListPartitionReassignments requests every in-flight partition
    Then foo has one pending row for partition 0 whose adding replicas contain exactly the new broker

  Scenario: cancel via null replicas reverts
    Given three plaintext brokers host RF2 foo partition 0 and the replacement keeps the first replica while adding the unassigned broker
    When the registered new replica shuts down before joining ISR and a surviving controller is elected
    And AlterPartitionReassignments targets the staying and new replicas through the active controller
    Then the assignment is in flight with the new broker adding and the original last broker removing
    When AlterPartitionReassignments cancels partition 0 with null replicas
    Then the partition reassignment response is exactly partition 0 error 0
    And cancellation restores the original ordered replica set and clears adding and removing replicas
