Feature: UUID wrapper string representation
  Scenario: UUID display respects Rust formatting options
    Given cluster and directory UUID wrappers contain 7
    When UUIDs are displayed with default width fill alignment and precision
    Then both UUID wrappers delegate text and formatting to their local codec

  Scenario: The UUID codec preserves values and descriptive parse errors
    Given cluster UUID 7 and directory UUID 9
    When UUID strings and JSON are encoded and decoded
    Then UUID strings and JSON retain their values and local codec errors
