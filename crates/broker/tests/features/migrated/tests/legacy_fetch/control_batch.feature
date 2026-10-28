Feature: Fetch downconversion for legacy consumers
  Scenario: Legacy fetch omits transaction control batches without returning null records
    Given a legacy-enabled broker has a committed transaction and its control marker
    When Fetch v3 starts at the transaction marker offset
    Then the legacy partition succeeds with an empty nonnull record set
