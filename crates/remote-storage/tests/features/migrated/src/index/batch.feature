Feature: src index batch

  Scenario Outline: tests: first record at or after timestamp refuses only the records kafka reads
    Then index batch tests: first record at or after timestamp refuses only the records kafka reads for <example>

    Examples:
      | example                       |
      | found before oversized batch  |
      | oversized record is match     |
      | oversized record before match |
      | batch below offset floor      |
      | no limit                      |
      | uncompressed batch            |
