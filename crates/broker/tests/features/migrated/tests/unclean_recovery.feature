Feature: Unclean recovery prefers complete eligible replicas and excludes witnesses

  Background:
    Given an offline aggressive-recovery partition whose broker 2 log is longest and broker 3 log is shorter

  Scenario: unclean recovery elects longest log replica
    When the active metadata controller receives an UNCLEAN election for t partition 0
    Then the unclean election succeeds for exactly partition zero
    And broker 2 becomes the recovered partition's only ISR member and leader

  Scenario: unclean recovery prefers an eligible leader replica
    Given the partition's eligible-leader-replica set contains broker 3
    When the active metadata controller receives an UNCLEAN election for t partition 0
    Then the unclean election succeeds for exactly partition zero
    And broker 3 becomes the recovered partition's only ISR member and leader

  Scenario: unclean recovery never elects a witness from the elr
    Given broker 3 is published as a witness before the eligible-replica set
    And the partition's eligible-leader-replica set contains broker 3
    When the active metadata controller receives an UNCLEAN election for t partition 0
    Then the unclean election succeeds for exactly partition zero
    And broker 2 becomes the recovered partition's only ISR member and leader
