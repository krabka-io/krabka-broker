Feature: Controlled shutdown drains broker leadership and handles controller-only roles

  Scenario: Shutdown drains every partition from a broker
    Given a three-broker cluster has four drain-me partitions replicated on all brokers and led by a noncontroller broker
    When the noncontroller broker requests controlled shutdown with a 30-second drain deadline
    Then shutdown succeeds and every surviving broker observes zero drain-me partitions led by the stopped broker

  Scenario: A controller-only node shuts down without waiting for a broker heartbeat
    Given an elected controller-only node has no data partitions or broker heartbeat client
    When the controller-only node requests controlled shutdown with a 10-second drain deadline
    Then the controller-only shutdown succeeds within the 20-second observation deadline
