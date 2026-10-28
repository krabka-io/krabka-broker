Feature: Share group lifecycle initializes and preserves per-topic durable state

  Scenario: lifecycle initializes share state
    Given a running share lifecycle coordinator for group g5
    And share lifecycle topic t5 has 3 partitions
    When the share group lifecycle initializes topic t5
    Then every share state partition of t5 exists with an uninitialized start offset

  Scenario: lifecycle metadata survives restart
    Given a running share lifecycle coordinator for group g6
    And share lifecycle topic t6 has 2 partitions
    When the share group lifecycle initializes topic t6
    Then every share state partition of t6 is present: true
    When the lifecycle broker restarts and the group's state summaries recover
    And the recovered member heartbeats again for topic t6
    Then every share state partition of t6 is present: true

  Scenario: delete groups deletes an empty share group and its state
    Given a running share lifecycle coordinator for group g-delete
    And share lifecycle topic t-delete has 2 partitions
    And member member-a has initialized its subscription to t-delete
    When an administrator deletes the share group
    Then the share lifecycle operation returns error 68
    And every share state partition of t-delete is present: true
    When share lifecycle member member-a leaves at epoch minus one
    Then the share lifecycle operation returns error 0
    When an administrator deletes the share group
    Then the share lifecycle operation returns error 0
    And every share state partition of t-delete is present: false
    When an administrator deletes the share group
    Then the share lifecycle operation returns error 69
    And every share state partition of t-delete is present: false

  Scenario: a deleted topic loses its share state
    Given a running share lifecycle coordinator for group g-doomed
    And share lifecycle topic doomed has 2 partitions
    And share lifecycle topic kept has 2 partitions
    And member member-a has initialized its subscription to doomed,kept
    When an administrator deletes share lifecycle topic doomed
    Then the share lifecycle operation returns error 0
    When member member-a heartbeats subscribed to doomed,kept until topic doomed state is present false
    Then every share state partition of kept is present: true

  Scenario: unassigned partitions keep their share state
    Given a running share lifecycle coordinator for group g-keep-leave
    And share lifecycle topic a has 2 partitions
    And share lifecycle topic b has 2 partitions
    And member member-a has initialized its subscription to a
    And member member-b has joined its subscription to b without waiting for state initialization
    When share lifecycle member member-a leaves at epoch minus one
    Then the share lifecycle operation returns error 0
    When member member-b heartbeats subscribed to b until topic b state is present true
    Then every share state partition of a is present: true

  Scenario: a member moving subscriptions keeps the old topic state
    Given a running share lifecycle coordinator for group g-keep-move
    And share lifecycle topic a has 2 partitions
    And share lifecycle topic b has 2 partitions
    And member member-a has initialized its subscription to a
    When member member-a heartbeats subscribed to b until topic b state is present true
    Then every share state partition of a is present: true

  Scenario: every initialized partition starts uninitialized
    Given a running share lifecycle coordinator for group g-grow
    And share lifecycle topic t-grow has 2 partitions
    And member member-a has initialized its subscription to t-grow
    When an administrator grows share lifecycle topic t-grow to 4 partitions
    Then the share lifecycle operation returns error 0
    When member member-a heartbeats subscribed to t-grow until topic t-grow state is present true
    Then every share state partition of t-grow exists with an uninitialized start offset
