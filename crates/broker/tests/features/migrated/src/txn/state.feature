Feature: Transaction states follow Kafka's transaction state machine

  Rule: A transaction moves only along Kafka's state transitions

    Scenario Outline: A transaction moves to the next state of its lifecycle
      Given a transaction is in state <from>
      When the coordinator checks a move to state <to>
      Then the move is allowed

      Examples:
        | case                                         | from           | to      |
        | the first partition opens an empty one       | Empty          | Ongoing |
        | a completed transactional id is reused       | CompleteCommit | Empty   |

    Scenario Outline: A transaction does not skip a state of its lifecycle
      Given a transaction is in state <from>
      When the coordinator checks a move to state <to>
      Then the move is refused

      Examples:
        | case                                         | from    | to             |
        | an empty transaction has nothing to prepare  | Empty   | PrepareCommit  |
        | an open transaction completes only prepared  | Ongoing | CompleteCommit |

  Rule: A transaction state persists as its Kafka status byte

    Scenario Outline: A transaction state survives the transaction log
      Given a transaction is in state <state>
      When the coordinator round-trips the transaction through the transaction log
      Then the transaction log holds status byte <status>
      And the coordinator reads back state <state>

      Examples:
        | case            | state          | status |
        | empty           | Empty          | 0      |
        | ongoing         | Ongoing        | 1      |
        | prepare commit  | PrepareCommit  | 2      |
        | prepare abort   | PrepareAbort   | 3      |
        | complete commit | CompleteCommit | 4      |
        | complete abort  | CompleteAbort  | 5      |
        | dead            | Dead           | 6      |

  Rule: A status byte Krabka does not model recovers no state

    Scenario Outline: An unmodeled status byte recovers no transaction state
      Given a transaction log record contains Kafka status byte <status>
      When the Kafka transaction status byte is recovered
      Then the status byte has no modeled transaction state

      Examples:
        | case                         | status |
        | prepare epoch fence          | 7      |
        | below the first status byte  | -1     |
