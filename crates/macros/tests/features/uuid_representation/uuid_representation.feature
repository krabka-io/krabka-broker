Feature: UUID wrapper string representation
  Scenario Outline: UUID display respects Rust formatting options
    Given cluster and directory UUID wrappers contain <value>
    When UUIDs are displayed with default width fill alignment and precision
    Then both UUID wrappers delegate text and formatting to their local codec

    Examples:
      | case           | value |
      | a single digit | 7     |
      | several digits | 123   |

  Scenario Outline: The UUID codec preserves values and descriptive parse errors
    Given cluster UUID <cluster> and directory UUID <directory>
    When UUID strings and JSON are encoded and decoded
    Then UUID strings and JSON retain their values and local codec errors

    Examples:
      | case                                 | cluster | directory |
      | distinct single-digit values         | 7       | 9         |
      | several digits with a zero directory | 123     | 0         |
