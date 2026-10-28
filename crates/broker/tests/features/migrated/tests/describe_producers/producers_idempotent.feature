Feature: Producer inspection reflects actual idempotent append history
  Background:
    Given a standalone broker is ready to inspect producer state

  Scenario: empty partition returns no active producers
    Given producer topic fresh exists with one partition
    When the client describes producers for topic fresh partitions 0
    Then fresh partition 0 succeeds with no active producer rows

  Scenario: after idempotent produce describe returns the producer
    Given producer topic t exists with one partition
    And 1 idempotent producer identities have been claimed
    And each claimed producer appended values a,b,c to t starting at sequence 0
    When the client describes producers for topic t partitions 0
    Then the single producer snapshot matches its identity epoch and sequence 2 without transaction fields

  Scenario: multiple producers on same partition all surfaced
    Given producer topic shared exists with one partition
    And 2 idempotent producer identities have been claimed
    And each claimed producer appended values x to shared starting at sequence 0
    When the client describes producers for topic shared partitions 0
    Then the shared partition snapshot contains exactly both claimed producer identities
