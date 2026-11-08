Feature: A token bucket grants whole tokens from its balance

  A caller that meters one token per event, such as one per new connection,
  asks the bucket for tokens and holds the event back when it grants none.
  The broker's KIP-612 connection rates do not use it: an ip entity's
  connection_creation_rate is a sampled client quota, documented in
  tests/features/client_quota.

  Rule: A grant takes whole tokens from the balance

    Scenario: A full bucket grants its whole burst
      Given a connection quota of 2 connections per second with a burst of 2
      When the acceptor asks for 3 connections
      Then the acceptor is granted 2 connections

    Scenario: A spent bucket grants nothing until it refills
      Given a connection quota of 2 connections per second with a burst of 2
      And the acceptor has been granted 2 connections
      When the acceptor asks for 1 connections
      Then the acceptor is granted 0 connections

  Rule: A bucket in debt grants nothing

    Scenario: The refill repays recorded connections before it grants one
      Given a connection quota of 2 connections per second with a burst of 2
      And 3 connections have been recorded
      And 500 ms have passed
      When the acceptor asks for 1 connections
      Then the acceptor is granted 0 connections
