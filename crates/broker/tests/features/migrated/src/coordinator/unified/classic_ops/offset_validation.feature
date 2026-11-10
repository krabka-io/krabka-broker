Feature: Classic offset commit membership fencing
  Scenario Outline: Kafka classic groups validate state member generation and static instance identity in order
    Given the classic group has m1 and m2 with static instance i1 pinned to m1 and state <state>
    When classic member <member> instance <instance> commits with generation <generation>
    Then the classic offset commit returns error code <code>

    Examples:
      | case                                           | state      | member | instance | generation | code |
      | an admin commit to an empty group              | empty      | empty  | absent   | -1         | 0    |
      | a negative generation skips the member check   | empty      | ghost  | absent   | -1         | 0    |
      | an anonymous commit to a group with members    | stable     | empty  | absent   | -1         | 25   |
      | a group preparing a rebalance has members      | preparing  | empty  | absent   | -1         | 25   |
      | an anonymous commit at the current generation  | stable     | empty  | absent   | current    | 25   |
      | a generation on an empty group                 | empty      | empty  | absent   | 0          | 25   |
      | an unknown member                              | stable     | ghost  | absent   | current    | 25   |
      | a member with a negative generation            | stable     | m1     | absent   | -1         | 22   |
      | a member with another generation               | stable     | m1     | absent   | 99         | 22   |
      | a member at the current generation             | stable     | m1     | absent   | current    | 0    |
      | a member while the group prepares a rebalance  | preparing  | m1     | absent   | current    | 0    |
      | a member while the group completes a rebalance | completing | m1     | absent   | current    | 27   |
      | a held instance without its member id          | stable     | empty  | i1       | current    | 82   |
      | a held instance under another member id        | stable     | m2     | i1       | current    | 82   |
      | an instance no member holds                    | stable     | m1     | i9       | current    | 25   |
      | a member with its own instance                 | stable     | m1     | i1       | current    | 0    |
