Feature: tests describe producers producers idempotent

  Scenario: empty partition returns no active producers
    Then describe producers producers idempotent: empty partition returns no active producers

  Scenario: after idempotent produce describe returns the producer
    Then describe producers producers idempotent: after idempotent produce describe returns the producer

  Scenario: multiple producers on same partition all surfaced
    Then describe producers producers idempotent: multiple producers on same partition all surfaced
