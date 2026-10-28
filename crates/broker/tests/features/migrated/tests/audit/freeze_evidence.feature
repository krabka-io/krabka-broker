Feature: Audit evidence independently reconstructs a signed two person incident
  Background:
    Given alice froze orders and bob and carol approved its thaw while a second proposal was withdrawn

  Scenario: every kfc9 phase writes one chained audit record
    When the auditor fetches audit records through operation thaw_topic_freeze.consumed
    Then every freeze proposal approval thaw and withdrawal phase has successful audit evidence
    When the auditor reads the chained sequence numbers
    Then at least five audit sequence numbers form one contiguous duplicate free chain
    When the broker shuts down and the offline auditor verifies its durable audit partition
    Then the offline hash chain is valid and includes every record fetched online

  Scenario: the proposal id joins each approval to the transition it authorized
    When the auditor fetches audit records through operation thaw_topic_freeze.applied
    Then the thaw proposal id joins exactly alice proposal and bob and carol approvals with ordered counterparties

  Scenario: a signed freeze reverifies from the audit topic with no metadata image
    When the auditor fetches audit records through operation set_topic_freeze.applied
    And the auditor rebuilds the signed bytes from the freeze event and verifies both freeze and thaw directions
    Then the event preserves alice key signature timestamp and principal and verifies only as a freeze
