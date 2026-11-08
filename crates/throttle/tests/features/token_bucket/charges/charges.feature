Feature: A token bucket charges every recording in full and keeps the overage as debt

  The token bucket refills at its rate and holds a burst of it. The broker's
  quotas are not token buckets: the sampled KIP-13 and KIP-124 client quotas
  and the KIP-612 connection_creation_rate are documented in
  tests/features/client_quota.

  A bucket metered in bytes counts one token per byte. A recording takes its
  whole charge whether or not the balance covers it. What the balance cannot
  cover is debt, and a caller that throttles on the debt waits for the debt
  divided by the rate. The scenarios below use a bucket of 1000 bytes per
  second with a burst of 11 seconds of it, 11000 bytes, unless they say
  otherwise.

  Rule: A charge the balance covers leaves no debt

    Scenario Outline: A charge within the burst leaves no debt
      Given a token bucket of 1000 bytes per second
      When <charged> bytes are recorded
      Then the bucket owes 0 bytes

      Examples:
        | case                    | charged |
        | part of the burst       | 4000    |
        | exactly the whole burst | 11000   |

  Rule: A charge past the balance is taken in full and the rest is owed

    Scenario: One byte past the burst is owed
      Given a token bucket of 1000 bytes per second
      When 11001 bytes are recorded
      Then the bucket owes 1 bytes

    Scenario: The next charge adds to the debt of the one before
      Given a token bucket of 1000 bytes per second
      And 15000 bytes have been recorded
      When 1000 bytes are recorded
      Then the bucket owes 5000 bytes

  Rule: A bounded charge keeps no more debt than the bound repays

    Scenario: A bounded charge owes at most one bound of debt
      Given a token bucket of 1000 bytes per second
      When 20000 bytes are recorded under a throttle bound of 1 seconds
      Then the bucket owes 1000 bytes

  Rule: A refund gives a charge back

    Scenario: A refund repays the debt before it refills the balance
      Given a token bucket of 1000 bytes per second
      And 15000 bytes have been recorded
      When 15000 bytes are refunded
      Then the bucket can take 11000 bytes before it owes anything

  Rule: A bucket without a rate never charges

    Scenario: An unlimited bucket owes nothing
      When 1000000000 bytes are recorded
      Then the bucket owes 0 bytes
