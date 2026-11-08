Feature: Kafka follower caught-up credit
  A follower that reaches the current leader end is caught up now. A follower
  keeping pace with the previous fetch's leader end is credited at that fetch's
  time; otherwise it remains uncaught-up.

  Scenario: A first fetch short of the log end receives no caught-up credit
    Given a follower that has never fetched
    When the follower fetches in this order:
      | at_ms | fetch_offset | leader_end |
      | 100   | 5            | 10         |
    Then its fetch offset is 5, last fetch is 100 ms, caught-up time is 0 ms, and last observed leader end is 10

  Scenario: A first fetch at the log end is caught up immediately
    Given a follower that has never fetched
    When the follower fetches in this order:
      | at_ms | fetch_offset | leader_end |
      | 100   | 10           | 10         |
    Then its fetch offset is 10, last fetch is 100 ms, caught-up time is 100 ms, and last observed leader end is 10

  Scenario: Keeping pace with appends earns credit at the previous fetch
    Given a follower that has never fetched
    When the follower fetches in this order:
      | at_ms | fetch_offset | leader_end |
      | 100   | 5            | 10         |
      | 200   | 10           | 20         |
    Then its fetch offset is 10, last fetch is 200 ms, caught-up time is 100 ms, and last observed leader end is 20

  Scenario: Remaining below the previous leader end receives no credit
    Given a follower that has never fetched
    When the follower fetches in this order:
      | at_ms | fetch_offset | leader_end |
      | 100   | 5            | 10         |
      | 200   | 9            | 20         |
    Then its fetch offset is 9, last fetch is 200 ms, caught-up time is 0 ms, and last observed leader end is 20

  Scenario: Catching up to the current leader end refreshes the caught-up time
    Given a follower that has never fetched
    When the follower fetches in this order:
      | at_ms | fetch_offset | leader_end |
      | 100   | 5            | 10         |
      | 200   | 10           | 20         |
      | 300   | 20           | 20         |
    Then its fetch offset is 20, last fetch is 300 ms, caught-up time is 300 ms, and last observed leader end is 20
