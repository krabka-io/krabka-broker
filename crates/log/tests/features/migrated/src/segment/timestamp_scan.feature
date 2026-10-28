Feature: src segment timestamp scan

  Scenario Outline: record limit tests: a lookup by timestamp refuses only the records kafka reads
    Then segment timestamp scan record limit tests: a lookup by timestamp refuses only the records kafka reads for <example>

    Examples:
      | example                |
      | before oversized batch |
      | oversized match        |
      | gzip without limit     |
      | uncompressed           |
