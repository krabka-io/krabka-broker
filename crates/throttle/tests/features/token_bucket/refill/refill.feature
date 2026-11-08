Feature: A token bucket refills at its rate and repays debt first

  The bucket refills at its rate, up to its burst. The refill repays the debt
  before it adds to the balance, so a caller that throttles on the debt waits
  for everything recorded above the rate. The scenarios below use a bucket of
  1000 bytes per second with a burst of 11 seconds of it unless they say
  otherwise.

  Rule: The refill repays the debt at the bucket's rate

    Scenario Outline: Time passing repays the debt
      Given a token bucket of 1000 bytes per second
      And 15000 bytes have been recorded
      When <wait> ms pass
      Then the bucket owes <owed> bytes

      Examples:
        | case                        | wait | owed |
        | half the debt is repaid     | 2000 | 2000 |
        | the whole debt is repaid    | 4000 | 0    |
        | the refill outruns the debt | 9000 | 0    |

  Rule: A fractional rate is refilled as configured

    Scenario: Half a byte per second repays one byte in two seconds
      Given a token bucket of 0.5 bytes per second
      And 7 bytes have been recorded
      When 2000 ms pass
      Then the bucket owes 0.5 bytes

  Rule: A rate change keeps the debt and repays it at the new rate

    Scenario: A raised rate repays the same debt faster
      Given a token bucket of 1000 bytes per second
      And 15000 bytes have been recorded
      And the bucket's rate has been raised to 2000 bytes per second
      When 1000 ms pass
      Then the bucket owes 2000 bytes
