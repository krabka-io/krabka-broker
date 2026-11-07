Feature: Audit events survive an audit topic outage in the spool

  When the broker cannot write to __krabka_audit, it appends each chained
  record to the durable audit spool instead (NIST AU-5). The replay task
  drains the spool to the topic once the topic accepts writes again. Spool
  mode is sticky: until the spool is empty, every new record goes behind the
  spooled ones, so the topic receives the chain in order.

  Rule: Spooled events reach the topic in chain order

    Scenario: Events spooled during an outage are replayed in order
      Given the audit topic is unavailable
      And the broker has written 3 audit events
      When the audit topic becomes available
      Then the audit topic holds the events at sequences 0, 1, 2

  Rule: Each spooled event reaches the topic exactly once

    Scenario: A replay cut short does not repeat an event
      Given the broker has spooled 4 audit events during an outage
      And a replay stopped after the audit topic accepted 2 of them
      When the audit topic becomes available
      Then the audit topic holds the events at sequences 0, 1, 2, 3

  Rule: While the spool holds events, new events queue behind them

    Scenario: An event written before the replay waits behind the spooled event
      Given the broker has spooled an audit event during an outage
      And the audit topic has recovered before the next replay
      When the broker writes another audit event
      Then the audit topic holds no events
      And the spool holds 2 events
