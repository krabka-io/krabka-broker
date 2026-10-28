Feature: tests kip516 offsets

  Scenario Outline: offset commit and fetch by topic id round trip
    Then kip516 offsets: offset commit and fetch by topic id round trip for <example>

    Examples:
      | example      |
      | listed topic |
      | fetch all    |

  Scenario: offset fetch unresolved topic id returns unknown topic id
    Then kip516 offsets: offset fetch unresolved topic id returns unknown topic id

  Scenario: offset commit unresolved topic id returns unknown topic id
    Then kip516 offsets: offset commit unresolved topic id returns unknown topic id
