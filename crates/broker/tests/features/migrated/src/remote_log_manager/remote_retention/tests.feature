Feature: Remote retention deletes the oldest remote segments that a retention limit breaches

  Rule: A segment expires once its maximum timestamp is strictly before now minus retention.ms

    # Kafka's RemoteLogManager isSegmentBreachedByRetentionTime, under a
    # RetentionTimeData that exists only while now - retention.ms >= 0.
    Scenario Outline: Time retention at millisecond 10000 when <case>
      Given finished remote segments of 100 bytes with maximum timestamps <timestamps>
      And the remote retention limits are <limits>
      When retention evaluates the mutable archive at millisecond 10000
      Then retention deletes the segments starting at <deleted>

      Examples:
        | case                                                     | timestamps     | limits   | deleted    |
        | an unknown timestamp is compared like any other          | -1             | 1 ms     | 0          |
        | a timestamp just before the cutoff expires               | 8999           | 1000 ms  | 0          |
        | a timestamp at the cutoff stays                          | 9000           | 1000 ms  | no segment |
        | a window longer than the clock disables the time axis    | -1             | 10001 ms | no segment |
        | a disabled time axis ignores even a timestamp below -1   | -2             | 10001 ms | no segment |
        | a window equal to the clock leaves a cutoff of zero      | -1             | 10000 ms | 0          |
        | the oldest segments go until one inside the window       | 100, 200, 9500 | 500 ms   | 0, 10      |
        | an expired segment after a kept one stays                | 100, 9500, 200 | 500 ms   | 0          |

  Rule: A segment goes only while the bytes over retention.bytes still cover the whole segment

    # Kafka's RemoteLogManager isSegmentBreachedByRetentionSize.
    Scenario Outline: Size retention of three 100-byte segments when <case>
      Given finished remote segments of 100 bytes with maximum timestamps 100, 200, 300
      And the remote retention limits are <limits>
      When retention evaluates the mutable archive at millisecond 10000
      Then retention deletes the segments starting at <deleted>

      Examples:
        | case                            | limits      | deleted    |
        | 150 over deletes one, not two   | 150 bytes   | 0          |
        | 200 over deletes two            | 100 bytes   | 0, 10      |
        | 250 over deletes two, not three | 50 bytes    | 0, 10      |
        | 50 over deletes nothing         | 250 bytes   | no segment |
        | exactly at the budget keeps all | 300 bytes   | no segment |
        | under the budget keeps all      | 10000 bytes | no segment |
        | 300 over deletes all three      | 0 bytes     | 0, 10, 20  |

  Rule: Local bytes the tier does not hold yet count toward retention.bytes

    # Kafka's RemoteLogManager buildRetentionSizeData adds onlyLocalLogSegmentsSize.
    Scenario Outline: Two 100-byte remote segments under a 200-byte budget when <case>
      Given finished remote segments of 100 bytes with maximum timestamps 100, 200
      And the remote retention limits are 200 bytes
      And the local log holds <local> bytes the tier does not hold yet
      When retention evaluates the mutable archive at millisecond 10000
      Then retention deletes the segments starting at <deleted>

      Examples:
        | case                                       | local | deleted    |
        | nothing local leaves the budget met        | 0     | no segment |
        | local bytes short of a segment delete none | 99    | no segment |
        | local bytes of one segment push one out    | 100   | 0          |

  Rule: Time and size retention delete what either breaches, and a time deletion pays down the size debt

    # Kafka's isSegmentBreachedByRetentionTime charges a time-expired segment
    # against the remaining breached size.
    Scenario Outline: One segment past a 500 ms window and two inside it when <case>
      Given finished remote segments of 100 bytes with maximum timestamps 100, 9900, 9900
      And the remote retention limits are <limits>
      When retention evaluates the mutable archive at millisecond 10000
      Then retention deletes the segments starting at <deleted>

      Examples:
        | case                                               | limits              | deleted |
        | 150 over: time takes 100, 50 cannot cover the next | 500 ms, 150 bytes   | 0       |
        | 200 over: time takes 100, size takes the next      | 500 ms, 100 bytes   | 0, 10   |
        | 50 over: time alone, the debt floors at zero       | 500 ms, 250 bytes   | 0       |
        | under budget: time alone                           | 500 ms, 10000 bytes | 0       |

  Rule: Retention deletes nothing without a limit or without a segment

    Scenario Outline: Nothing is deleted when <case>
      Given finished remote segments of 100 bytes with maximum timestamps <timestamps>
      And the remote retention limits are <limits>
      When retention evaluates the mutable archive at millisecond 10000
      Then retention deletes the segments starting at <deleted>

      Examples:
        | case                                 | timestamps | limits       | deleted    |
        | an old segment has neither limit set | 100        | no limit     | no segment |
        | an empty archive has both limits set | no segment | 1 ms, 1 byte | no segment |
