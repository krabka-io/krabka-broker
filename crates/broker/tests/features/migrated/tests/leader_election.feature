Feature: tests leader election

  Scenario: broker death elects new leader
    Then leader election: broker death elects new leader

  Scenario: unclean failover recovers after a real broker restart
    Then leader election: unclean failover recovers after a real broker restart

  Scenario: acks all completes after isr shrink
    Then leader election: acks all completes after isr shrink

  Scenario: isr expand on catchup
    Then leader election: isr expand on catchup

  Scenario: produce during leader failover
    Then leader election: produce during leader failover

  Scenario: a leader restarted inside its session is fenced and loses leadership
    Then leader election: a leader restarted inside its session is fenced and loses leadership

  Scenario: a controller leader restarted inside its session loses its partition leadership
    Then leader election: a controller leader restarted inside its session loses its partition leadership
