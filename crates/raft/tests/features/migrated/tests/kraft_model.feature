Feature: Exhaustive bounded KRaft consensus safety
  Election-only models cover leader and log agreement. Append models additionally
  cover committed-log linearizability and leader completeness. Fault models permit
  message loss, duplication and one crashed voter. Stateless appenders exercise
  the append-via path rather than only direct leader appends.

  Scenario Outline: Every bounded consensus configuration exhausts its original reachable graph safely
    Given the bounded consensus configuration <configuration>
    When every reachable consensus state is explored breadth first
    Then the model search reaches neither its state-count nor its depth backstop
    And the exact reachable-state count is <states>
    And all consensus safety properties and progress witnesses hold

    Examples:
      | case                                  | configuration                        | states  |
      | election safety of three voters       | three-voter-election-safety           | 27343   |
      | linearizable appends of two voters    | two-voter-linearizable-two-appends    | 70037   |
      | message loss, duplication and a crash | three-voter-loss-duplication-one-crash | 3224882 |
      | an append with a crashed voter        | three-voter-one-append-one-crash      | 2614970 |
      | appends through a stateless appender  | two-voter-stateless-two-appends       | 344543  |
