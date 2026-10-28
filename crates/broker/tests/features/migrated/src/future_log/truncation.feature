Feature: Future log migration reconciles leader epochs before swapping logs
  The future replica must remain a prefix of the current replica's history.
  Kafka epoch -1 represents a current log that cannot place the requested epoch.

  Background:
    Given the future log has epoch 1 starting at 0 and epoch 3 starting at 10
    And the future log ends at 15 and the initial truncation offset is 10

  Scenario Outline: Reconcile the current log's epoch answer
    Given the current log reports epoch <epoch> ending at <end>
    When the migration chooses its next future-log truncation state
    Then the future-log truncation offset is <offset> and reconciliation completed is <completed>

    Examples:
      | epoch | end | offset | completed |
      | -1 | -1 | 10 | false |
      | 3 | 12 | 12 | true |
      | 3 | 40 | 15 | true |
      | 2 | 12 | 10 | false |
      | 1 | 4 | 4 | true |
      | 0 | 6 | 0 | true |
