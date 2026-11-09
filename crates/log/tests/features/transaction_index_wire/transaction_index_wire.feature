Feature: Kafka transaction index wire compatibility

  Rule: Each aborted transaction is stored as Kafka's fixed 34-byte record

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

  Rule: A fetch reports the aborted transactions that overlap its offsets, up to but excluding its end

    Scenario Outline: A fetch that reaches into an aborted transaction reports it
      Given an aborted transaction spanning offsets 10 to 20 has been indexed
      When a fetch from offset <start> up to offset <end> collects aborted transactions
      Then the fetch reports the aborted transaction

      Examples:
        | case                                | start | end |
        | a fetch reaching one past its start | 0     | 11  |
        | a fetch starting on its last offset | 20    | 30  |

    Scenario Outline: A fetch that does not reach an aborted transaction reports none
      Given an aborted transaction spanning offsets 10 to 20 has been indexed
      When a fetch from offset <start> up to offset <end> collects aborted transactions
      Then the fetch reports no aborted transaction

      Examples:
        | case                                        | start | end |
        | a fetch ending where the transaction begins | 0     | 10  |
        | a fetch starting past its last offset       | 21    | 30  |
        | an empty fetch range                        | 10    | 10  |
        | an inverted fetch range                     | 20    | 10  |
