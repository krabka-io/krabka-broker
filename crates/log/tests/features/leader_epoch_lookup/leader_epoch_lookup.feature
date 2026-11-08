Feature: Kafka endOffsetFor over a recorded leader epoch history
  Epoch histories use epoch:start-offset pairs; -1 denotes an undefined answer.

  Scenario Outline: An epoch query returns Kafka's floor epoch and end offset
    Given leader epoch lookup history <history>
    When epoch <requested> is looked up with log end <log_end>
    Then the epoch lookup returns epoch <epoch> ending at offset <offset>

    Examples:
      | case                                       | history        | requested | log_end | epoch | offset |
      | an empty history                           | empty          | 0         | 9       | -1    | -1     |
      | an undefined requested epoch               | 0:0,1:50       | -1        | 100     | -1    | -1     |
      | the latest epoch ends at the log end       | 0:0,1:50       | 1         | 100     | 1     | 100    |
      | the first epoch ends where the next starts | 0:0,1:50,2:100 | 0         | 200     | 0     | 50     |
      | a middle epoch ends where the next starts  | 0:0,1:50,2:100 | 1         | 200     | 1     | 100    |
      | an unrecorded epoch takes the floor epoch  | 0:0,5:100      | 3         | 200     | 0     | 100    |
      | an epoch newer than the history            | 0:0,1:50       | 7         | 100     | -1    | -1     |
      | an epoch older than the earliest entry     | 3:30,4:40      | 1         | 100     | 1     | 30     |
