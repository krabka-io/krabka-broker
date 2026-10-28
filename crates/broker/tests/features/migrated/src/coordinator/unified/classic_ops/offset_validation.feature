Feature: Classic offset commit membership fencing
  Scenario Outline: Kafka classic groups validate state member generation and static instance identity in order
    Given the classic group has m1 and m2 with static instance i1 pinned to m1 and state <state>
    When classic member <member> instance <instance> commits with generation <generation>
    Then the classic offset commit returns error code <code>
    Examples:
      | state | member | instance | generation | code |
      | empty | empty | absent | -1 | 0 |
      | empty | ghost | absent | -1 | 0 |
      | stable | empty | absent | -1 | 25 |
      | preparing | empty | absent | -1 | 25 |
      | stable | empty | absent | current | 25 |
      | empty | empty | absent | 0 | 25 |
      | stable | ghost | absent | current | 25 |
      | stable | m1 | absent | -1 | 22 |
      | stable | m1 | absent | 99 | 22 |
      | stable | m1 | absent | current | 0 |
      | preparing | m1 | absent | current | 0 |
      | completing | m1 | absent | current | 27 |
      | stable | empty | i1 | current | 82 |
      | stable | m2 | i1 | current | 82 |
      | stable | m1 | i9 | current | 25 |
      | stable | m1 | i1 | current | 0 |
