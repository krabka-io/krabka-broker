Feature: Initial streams group epochs
  A newly created streams group starts at Kafka's initial group and target
  assignment epochs before any member joins or assignments are reconciled.

  Scenario: A new streams group is empty at initial epoch one
    Given a previously unseen streams group ID g
    When the streams coordinator creates that group
    Then the streams group is Empty with group, assignment, and target epochs all 1
