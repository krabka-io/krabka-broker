Feature: Kafka endOffsetFor over a recorded leader epoch history
  Epoch histories use epoch:start-offset pairs; -1 denotes an undefined answer.

  Scenario Outline: An epoch query returns Kafka's floor epoch and end offset
    Given leader epoch lookup history <history>
    When epoch <requested> is looked up with log end <log_end>
    Then the epoch lookup returns epoch <epoch> ending at offset <offset>

    Examples:
      | history          | requested | log_end | epoch | offset |
      | empty            | 0         | 9       | -1    | -1     |
      | 0:0,1:50         | -1        | 100     | -1    | -1     |
      | 0:0,1:50         | 1         | 100     | 1     | 100    |
      | 0:0,1:50,2:100   | 0         | 200     | 0     | 50     |
      | 0:0,1:50,2:100   | 1         | 200     | 1     | 100    |
      | 0:0,5:100        | 3         | 200     | 0     | 100    |
      | 0:0,1:50         | 7         | 100     | -1    | -1     |
      | 3:30,4:40        | 1         | 100     | 1     | 30     |
