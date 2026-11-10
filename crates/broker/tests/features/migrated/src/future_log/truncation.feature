Feature: Future log migration reconciles leader epochs before swapping logs
  The future replica must remain a prefix of the current replica's history.
  Kafka epoch -1 represents a current log that cannot place the requested epoch.

  Rule: The current log's answer for the future log's latest epoch sets the next truncation offset

    Scenario Outline: Reconcile the current log's epoch answer
      Given the future log has epoch 1 starting at 0 and epoch 3 starting at 10
      And the future log ends at 15 and the initial truncation offset is 10
      And the current log reports epoch <epoch> ending at <end>
      When the migration chooses its next future-log truncation state
      Then the future-log truncation offset is <offset> and reconciliation completed is <completed>

      Examples:
        | case                                         | epoch | end | offset | completed |
        | the current log cannot place the epoch       | -1    | -1  | 10     | false     |
        | the current log ends the shared epoch first  | 3     | 12  | 12     | true      |
        | the future log end bounds the cut            | 3     | 40  | 15     | true      |
        | the future log knows only a smaller epoch    | 2     | 12  | 10     | false     |
        | an older shared epoch ends first             | 1     | 4   | 4      | true      |
        | an epoch older than the future log's history | 0     | 6   | 0      | true      |

  Rule: The future log keeps only the history it shares with the current log

    Scenario Outline: The future log is cut back to the history both logs hold
      Given a future log whose records carry leader epochs <future>
      And a current log whose records carry leader epochs <current>
      When the future log is reconciled with the current log
      Then the future log ends at offset <end> while the current log keeps every record

      Examples:
        | case                                           | future    | current | end |
        | a tail the current log replaced is cut         | 1,1,1,1   | 1,1,2,2 | 2   |
        | a prefix of the current log is left alone      | 1,1       | 1,1,1,2 | 2   |
        | epochs the current log never had are dropped   | 1,1,2,3,3 | 1,1     | 2   |
        | records without an epoch are emptied           | -1,-1     | 1,1     | 0   |
        | an empty future log stays empty                | nothing   | 1,1     | 0   |
