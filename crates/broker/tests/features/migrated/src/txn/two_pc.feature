# The idle reaper's boundaries are illustrated in crates/verified, in
# tests/features/migrated/src/transaction/tests/idle_transaction_reaper_matches_kafka_timed_out_transactions.feature.
Feature: Transaction timeouts follow Kafka's bounds and KIP-939

  Rule: An ordinary transaction timeout is positive and at most the broker maximum

    Scenario Outline: A timeout within the range is stored as requested
      Given the maximum transaction timeout is 8000 milliseconds
      And the producer requests transaction timeout <requested> milliseconds
      When the broker resolves the ordinary transaction timeout
      Then the transaction timeout is <requested> milliseconds

      Examples:
        | case                | requested |
        | the smallest one    | 1         |
        | a typical one       | 5000      |
        | exactly the maximum | 8000      |

    Scenario Outline: A timeout outside the range is refused
      Given the maximum transaction timeout is 8000 milliseconds
      And the producer requests transaction timeout <requested> milliseconds
      When the broker resolves the ordinary transaction timeout
      Then the transaction timeout is refused with INVALID_TRANSACTION_TIMEOUT

      Examples:
        | case                                | requested   |
        | one above the maximum               | 8001        |
        | zero                                | 0           |
        | the most negative                   | -2147483648 |
        | the two-phase-commit sentinel       | 2147483647  |

  Rule: A two-phase-commit transaction is stored without a timeout, whatever the producer requests

    Scenario Outline: A two-phase-commit timeout is the no-timeout sentinel
      Given the maximum transaction timeout is 8000 milliseconds
      And the producer requests transaction timeout <requested> milliseconds
      When the broker resolves the two-phase-commit transaction timeout
      Then the transaction is stored with the no-timeout sentinel

      Examples:
        | case                  | requested |
        | above the maximum     | 30000     |
        | zero                  | 0         |
        | negative              | -5        |

  Rule: Only an ongoing transaction times out

    Scenario: An ongoing transaction open past its timeout is aborted
      Given a transaction in state Ongoing has been open longer than its timeout
      When the timeout reaper checks the transaction
      Then the reaper aborts the transaction

    Scenario Outline: A transaction that is not ongoing is never timed out
      Given a transaction in state <state> has been open longer than its timeout
      When the timeout reaper checks the transaction
      Then the reaper leaves the transaction alone

      Examples:
        | case                    | state          |
        | not yet begun           | Empty          |
        | committing              | PrepareCommit  |
        | aborting                | PrepareAbort   |
        | committed               | CompleteCommit |
        | aborted                 | CompleteAbort  |
        | expired                 | Dead           |
