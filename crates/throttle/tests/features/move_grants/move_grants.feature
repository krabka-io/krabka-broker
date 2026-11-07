Feature: Log directory moves are granted bytes at the throttle rate

  KIP-113. A broker copies a replica to another log directory in chunks, and
  replica.alter.log.dirs.io.max.bytes.per.second throttles the copy. Each copy
  step asks the throttle for a chunk and copies only the whole bytes it is
  granted. A step that is granted nothing waits for the next round.

  A throttle rate gives the bucket a burst of one second of traffic, and a
  bucket throttled for the first time starts with that burst.

  Rule: A request the balance covers is granted in full

    Scenario: A move that asks for less than one second of traffic gets all of it
      Given log directory move traffic is throttled at 1000 bytes per second
      When log directory move traffic asks for 400 bytes
      Then log directory move traffic is granted 400 bytes

  Rule: A request past the balance is granted only what the balance holds

    Scenario: A move that asks for more than one second of traffic gets one second of it
      Given log directory move traffic is throttled at 1000 bytes per second
      When log directory move traffic asks for 1500 bytes
      Then log directory move traffic is granted 1000 bytes

  Rule: The balance refills at the throttle rate up to one second of traffic

    Scenario Outline: A spent move throttle refills while the move waits
      Given log directory move traffic has spent a throttle of 1000 bytes per second
      And <wait> ms have passed
      When log directory move traffic asks for 5000 bytes
      Then log directory move traffic is granted <granted> bytes

      Examples:
        | case                                  | wait | granted |
        | no time has passed                    | 0    | 0       |
        | a quarter second refills a quarter    | 250  | 250     |
        | one second refills the whole burst    | 1000 | 1000    |
        | a longer wait is capped at the burst  | 5000 | 1000    |

  Rule: A move without a throttle rate is granted every request in full

    Scenario: An unthrottled move gets every byte it asks for
      When log directory move traffic asks for 1000000000 bytes
      Then log directory move traffic is granted 1000000000 bytes
