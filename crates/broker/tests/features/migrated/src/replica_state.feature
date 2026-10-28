Feature: Kafka follower progress and caught-up credit
  A follower earns credit from the fetch that proves it reached a leader log
  end. A short fetch retains earlier credit, and an overshoot never invents data.

  Scenario: Follower fetch progress credits caught-up time without inventing progress
    Given brokers 1 and 2 are installed in the ISR at time zero
    When follower 2 fetches offset 5 at 100 milliseconds while the leader ends at 10
    Then follower 2 has log end 5, last fetch 100, leader end 10, and caught-up time 0
    When follower 2 fetches offset 9 at 200 milliseconds while the leader ends at 20
    Then follower 2 has log end 9, last fetch 200, leader end 20, and caught-up time 0
    When follower 2 fetches offset 20 at 300 milliseconds while the leader ends at 30
    Then follower 2 has log end 20, last fetch 300, leader end 30, and caught-up time 200
    When follower 2 fetches offset 30 at 400 milliseconds while the leader ends at 30
    Then follower 2 has log end 30, last fetch 400, leader end 30, and caught-up time 400
    When follower 2 fetches offset 25 at 500 milliseconds while the leader ends at 40
    Then follower 2 has log end 25, last fetch 500, leader end 40, and caught-up time 400
    When follower 2 fetches offset 99 at 600 milliseconds while the leader ends at 40
    Then follower 2 has log end 40, last fetch 600, leader end 40, and caught-up time 600
