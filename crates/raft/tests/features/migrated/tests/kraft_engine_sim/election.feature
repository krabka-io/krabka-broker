Feature: Raft election converges for full quorums and bare majorities
  Scenario: Three engines elect exactly one leader
    Given three voters run with staggered election timeouts
    When the live voters converge within 10 seconds and report their quorum states
    Then exactly one live voter leads at an epoch of at least one
  Scenario: A bare majority elects with uniform timeouts
    Given only voters 1 and 2 run with uniform 200 millisecond election timeouts
    When the live voters converge within 8 seconds and report their quorum states
    Then exactly one live voter leads at an epoch of at least one
