Feature: A connection creation quota grants whole connections from its balance

  KIP-612. The broker meters max.connection.creation.rate and an ip entity's
  connection_creation_rate with a token bucket of one token per connection.
  The acceptor asks the bucket for one token per new connection and throttles
  the connection when the bucket grants none.

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
