Feature: Preferred leader elections restore replicas and report skipped partitions

  Background: Operator election cluster
    Given three plaintext brokers are registered for operator leader elections

  Scenario: A rejoined preferred replica can regain leadership
    Given foo-preferred partition 0 has replicas 1 and 2, and preferred broker 1 has stopped, rejoined and become electable in the ISR
    When the operator requests a preferred election of foo-preferred partition 0
    Then ElectLeaders returns partition 0 with success code 0
    And the surviving broker and rejoined broker both observe broker 1 as foo-preferred partition 0 leader

  Scenario: All-partitions elections omit partitions already on the preferred leader
    Given foo-all-partitions partition 0 already has preferred broker 1 as leader with replicas 1 and 2
    When the operator requests preferred elections for all partitions and then explicitly names foo-all-partitions partition 0
    Then the all-partitions response omits skipped rows and the named partition returns ELECTION_NOT_NEEDED code 84
