Feature: Produce record timestamps and reported time windows
  CreateTime topics validate producer timestamps against inclusive tolerances.
  Kafka's missing-timestamp exemption, LogAppendTime behavior and Java signed
  long overflow rules remain part of the wire compatibility contract.

  Background:
    Given the broker timestamp clock is 1000000 milliseconds

  Scenario Outline: Record admission follows the configured timestamp policy
    Given the topic uses <timestamp_type> with past tolerance <before> and future tolerance <after> milliseconds
    When the topic validates record timestamp <timestamp>
    Then the record timestamp is <admission>

    Examples:
      | timestamp_type | before    | after     | timestamp            | admission |
      # no window
      | CreateTime     | unbounded | unbounded | 0                    | accepted  |
      # inclusive past limit
      | CreateTime     | 100       | unbounded | 999900               | accepted  |
      # past limit exceeded
      | CreateTime     | 100       | unbounded | 999899               | rejected  |
      # inclusive future limit
      | CreateTime     | unbounded | 100       | 1000100              | accepted  |
      # future limit exceeded
      | CreateTime     | unbounded | 100       | 1000101              | rejected  |
      # past bound leaves future open
      | CreateTime     | 100       | unbounded | 1100000              | accepted  |
      # missing timestamp exempt
      | CreateTime     | 100       | 100       | -1                   | accepted  |
      # broker replaces timestamp
      | LogAppendTime  | 100       | 100       | 0                    | accepted  |
      # minimum long wraps forward
      | CreateTime     | 100       | 100       | -9223372036854775808 | rejected  |
      # stock future bound
      | CreateTime     | unbounded | 3600000   | -9223372036854775808 | rejected  |
      # wrapped past-only check
      | CreateTime     | 100       | unbounded | -9223372036854775808 | accepted  |
      # minimum difference negates to itself
      | CreateTime     | 100       | 100       | -9223372036853775808 | accepted  |

  Scenario Outline: Reported timestamp bounds retain Kafka's wrapping arithmetic
    Given the topic uses CreateTime with past tolerance <before> and future tolerance <after> milliseconds
    When the broker reports the allowed timestamp window
    Then the reported timestamp window is <low> through <high> inclusive

    Examples:
      | before    | after     | low                  | high                 |
      | 100       | 200       | 999900               | 1000200              |
      | unbounded | 3600000   | -9223372036853775807 | 4600000              |
      | 100       | unbounded | 999900               | -9223372036853775809 |
