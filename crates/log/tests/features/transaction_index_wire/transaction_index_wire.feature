Feature: Kafka transaction index wire compatibility
  Scenario: One aborted transaction writes Kafka's fixed record layout
    Given aborted transaction index entries
      | producer | start | last | stable |
      | 1000     | 5     | 9    | 10     |
    When the transaction index entries are appended and the index is reopened
    Then one transaction record has Kafka's exact 34-byte big-endian layout

  Scenario: Reopening preserves all transaction records at the exact entry size
    Given aborted transaction index entries
      | producer | start | last | stable |
      | 1000     | 0     | 3    | 4      |
      | 2000     | 4     | 6    | 7      |
      | 3000     | 8     | 10   | 11     |
    When the transaction index entries are appended and the index is reopened
    Then three 34-byte transaction records retain their complete entries
