Feature: Graceful controller resignation accelerates failover under production raft timings

  Scenario: Graceful resignation replaces the controller before the fetch timeout
    Given three combined brokers are registered using production controller fetch, election and heartbeat timings
    When the active controller shuts down gracefully and both survivors create a one-partition topic replicated twice
    Then the old controller drains successfully and both survivor mutations succeed within 1800ms of shutdown starting
