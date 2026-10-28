Feature: Broker-wide Kafka message size limits
  The command line accepts dimensioned byte values only when they represent
  a whole number of bytes in Kafka's nonnegative signed INT domain.

  Scenario Outline: Whole-byte limits within Kafka's INT range are accepted
    Given the operator supplies message limit <value>
    When the broker parses the message limit command line
    Then the broker-wide message limit is <bytes> bytes

    Examples:
      | value       | bytes      |
      | 0B          | 0          |
      | 2048B       | 2048       |
      | 2147483647B | 2147483647 |
      | 1MiB        | 1048576    |

  Scenario Outline: Limits outside Kafka's whole-byte INT domain prevent startup
    Given the operator supplies message limit <value>
    When the broker parses the message limit command line
    Then the invalid message limit is rejected

    Examples:
      | value       | boundary                  |
      | -1B         | negative                  |
      | 2147483648B | above signed INT maximum  |
      | 2GiB        | unit conversion overflows |
      | 1.5B        | fractional byte           |
