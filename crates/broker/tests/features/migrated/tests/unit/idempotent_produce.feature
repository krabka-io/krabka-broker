Feature: tests unit idempotent produce

  Scenario: idempotent produce dedups duplicate batch
    Then unit idempotent produce: idempotent produce dedups duplicate batch

  Scenario: out of order returns 45
    Then unit idempotent produce: out of order returns 45
