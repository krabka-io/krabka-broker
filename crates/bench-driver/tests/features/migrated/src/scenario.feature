Feature: src scenario

  Scenario: tests: stack broker pod regex distinguishes stacks
    Then scenario tests: stack broker pod regex distinguishes stacks

  Scenario Outline: tests: acks map to producer enum
    Then scenario tests: acks map to producer enum for <example>

    Examples:
      | example |
      | none    |
      | leader  |
      | all     |

  Scenario Outline: tests: compression maps to producer enum
    Then scenario tests: compression maps to producer enum for <example>

    Examples:
      | example |
      | none    |
      | gzip    |
      | snappy  |
      | lz4     |
      | zstd    |

  Scenario: tests: compression default is none
    Then scenario tests: compression default is none

  Scenario: tests: scenario yaml reads the operator form
    Then scenario tests: scenario yaml reads the operator form

  Scenario: tests: scenario yaml defaults every omitted field
    Then scenario tests: scenario yaml defaults every omitted field

  Scenario: tests: scenario yaml rejects a size without a unit
    Then scenario tests: scenario yaml rejects a size without a unit

  Scenario Outline: tests: scenario yaml rejects unrunnable magnitudes
    Then scenario tests: scenario yaml rejects unrunnable magnitudes for <example>

    Examples:
      | example                      |
      | negative message size        |
      | zero message size            |
      | negative key size            |
      | negative batch size          |
      | zero batch size              |
      | negative linger              |
      | negative duration            |
      | zero duration                |
      | negative warmup              |
      | negative failover kill after |

  Scenario Outline: tests: scenario yaml rejects an unrunnable fixed rate
    Then scenario tests: scenario yaml rejects an unrunnable fixed rate for <example>

    Examples:
      | example  |
      | negative |
      | zero     |

  Scenario: tests: scenario yaml admits zero where zero is runnable
    Then scenario tests: scenario yaml admits zero where zero is runnable

  Scenario: tests: fixed rate yaml parses an event rate
    Then scenario tests: fixed rate yaml parses an event rate

  Scenario: tests: failover yaml parses a kill offset
    Then scenario tests: failover yaml parses a kill offset

  Scenario: tests: scenario yaml round trips through the human form
    Then scenario tests: scenario yaml round trips through the human form

  Scenario: tests: run output json round trips
    Then scenario tests: run output json round trips

  Scenario: tests: run output json encodes measurements as exact integers
    Then scenario tests: run output json encodes measurements as exact integers

  Scenario: tests: absent optional resource fields decode as none
    Then scenario tests: absent optional resource fields decode as none
