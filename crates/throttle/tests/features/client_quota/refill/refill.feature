Feature: A client quota refills at its rate and repays debt first

  The bucket refills at the quota rate, up to one quota window of it. The
  refill repays what the client owes before it adds to the balance, so a
  client that sent past its quota is throttled by everything it sent above the
  bound. The scenarios below use a quota of 1000 bytes per second with an 11
  second window unless they say otherwise.

  Rule: The refill repays the debt at the quota rate

    Scenario Outline: Time passing repays what the client owes
      Given a client quota of 1000 bytes per second
      And the client has sent 15000 bytes
      When <wait> ms pass
      Then the client owes <owed> bytes

      Examples:
        | case                        | wait | owed |
        | half the debt is repaid     | 2000 | 2000 |
        | the whole debt is repaid    | 4000 | 0    |
        | the refill outruns the debt | 9000 | 0    |

  Rule: A fractional rate is enforced as configured

    Scenario: Half a byte per second repays one byte in two seconds
      Given a client quota of 0.5 bytes per second
      And the client has sent 7 bytes
      When 2000 ms pass
      Then the client owes 0.5 bytes

  Rule: A rate change keeps the debt and repays it at the new rate

    Scenario: A raised quota repays the same debt faster
      Given a client quota of 1000 bytes per second
      And the client has sent 15000 bytes
      And the quota has been raised to 2000 bytes per second
      When 1000 ms pass
      Then the client owes 2000 bytes
