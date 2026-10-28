Feature: src kafka log range

  Scenario Outline: tests: range steps follow batch progress to the end
    Then kafka log range tests: range steps follow batch progress to the end for <example>

    Examples:
      | example                 |
      | advances one            |
      | skips compacted offsets |
      | reaches end             |
      | passes end              |
      | no batch                |
      | no progress             |
      | regresses               |

  Scenario Outline: tests: retriable fetch failures are leader moves and broken connections
    Then kafka log range tests: retriable fetch failures are leader moves and broken connections for <example>

    Examples:
      | example              |
      | not leader           |
      | fenced epoch         |
      | unknown leader epoch |
      | leader unavailable   |
      | replica unavailable  |
      | storage error        |
      | offset unavailable   |
      | unknown topic        |
      | unknown server       |
      | disconnected         |
      | timeout              |
      | connection reset     |
      | connection refused   |
      | offset out of range  |
      | corrupt message      |
      | topic authorization  |
      | unsupported version  |
      | invalid config       |

  Scenario: tests: page records keep the fetch offset up to the range end
    Then kafka log range tests: page records keep the fetch offset up to the range end

  Scenario: tests: a retriable fetch failure looks the leader up again and resumes
    Then kafka log range tests: a retriable fetch failure looks the leader up again and resumes

  Scenario: tests: a partition without a leader is looked up again
    Then kafka log range tests: a partition without a leader is looked up again

  Scenario: tests: a fatal failure stops the read with its cause
    Then kafka log range tests: a fatal failure stops the read with its cause

  Scenario: tests: a fetch that makes no progress stalls the read
    Then kafka log range tests: a fetch that makes no progress stalls the read

  Scenario: tests: a visitor error stops the read before the next record
    Then kafka log range tests: a visitor error stops the read before the next record

  Scenario: tests: an empty read sends nothing
    Then kafka log range tests: an empty read sends nothing

  Scenario Outline: tests: out of range read sends nothing
    Then kafka log range tests: out of range read sends nothing for <example>

    Examples:
      | example     |
      | negative    |
      | upper bound |
