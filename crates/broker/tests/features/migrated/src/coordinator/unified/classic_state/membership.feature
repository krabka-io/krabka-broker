Feature: Classic member expiry while JoinGroup waits
  Scenario Outline: Waiting members use Kafka session and new-member join timeout rules
    Given m1 is new <is_new> waiting in JoinGroup <waiting> with ten second session and last heartbeat age <age>
    When the coordinator expires inactive classic members with three second initial rebalance delay
    Then the expired member list is <members>
    Examples:
      | is_new | waiting | age | members |
      | false | true | 60 | none |
      | false | false | 60 | m1 |
      | false | false | 5 | none |
      | true | true | 60 | none |
      | true | true | new-member-timeout-plus-one | m1 |
