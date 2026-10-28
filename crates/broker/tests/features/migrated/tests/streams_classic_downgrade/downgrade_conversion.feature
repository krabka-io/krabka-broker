Feature: Drained streams groups convert to classic and retain committed offsets through recovery

  Scenario: drained streams group downgrades and preserves offsets
    Given a downgrade broker holds one-partition topic in for group g
    When a streams member joins the downgrade group and converges on its single task
    Then the downgrade group has a healthy live Streams member
    When the empty-group waiter observes the live streams group for 75 milliseconds
    Then the empty-group waiter times out while the streams member remains live
    When the streams downgrade member commits input offset forty-two
    And the streams member leaves and the downgrade group drains
    And a classic JoinGroup synchronizes the drained streams group
    Then the drained group becomes Classic-typed
    When the classic client fetches the downgraded group's input offset
    Then the downgraded group's committed offset is forty-two without a partition error

  Scenario: downgrade survives restart
    Given a durable downgrade broker holds topic in4 for group g4
    When a streams member joins the downgrade group and converges on its single task
    Then the downgrade group has a healthy live Streams member
    When the streams downgrade member commits input offset forty-two
    And the streams member leaves and the downgrade group drains
    And a classic JoinGroup synchronizes the drained streams group
    Then the drained group becomes Classic-typed
    When the downgraded broker restarts on the same data directory
    Then replay seeds a classic actor and does not restore the group as Streams
    When the classic client fetches the downgraded group's input offset
    Then the downgraded group's committed offset is forty-two without a partition error
