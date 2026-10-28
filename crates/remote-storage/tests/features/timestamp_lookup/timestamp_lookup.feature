Feature: Remote timestamp lookup decompression limits
  Scenario Outline: Only decompressed records read by the lookup face the record limit
    Given a <codec> remote segment with a 1000-byte middle record between offsets 10 and 12
    And a remote timestamp lookup starts at offset 10 for timestamp <target> with limit <limit>
    When the first matching remote record is located
    Then the remote record at offset <offset> and timestamp <timestamp> is returned

    Examples:
      | codec        | target | limit     | offset | timestamp |
      | gzip         | 1000   | 100       | 10     | 1000      |
      | gzip         | 1100   | unlimited | 11     | 1100      |
      | uncompressed | 1100   | 100       | 11     | 1100      |

  Scenario Outline: An oversized record encountered before or at a match is refused
    Given a gzip remote segment with a 1000-byte middle record between offsets 10 and 12
    And a remote timestamp lookup starts at offset 10 for timestamp <target> with limit 100
    When the first matching remote record is located
    Then a 1007-byte record exceeds the 100-byte decompression bound

    Examples:
      | target |
      | 1100   |
      | 1200   |

  Scenario: A batch below the requested offset floor is skipped
    Given a gzip remote segment with a 1000-byte middle record between offsets 10 and 12
    And a remote timestamp lookup starts at offset 13 for timestamp 1100 with limit 100
    When the first matching remote record is located
    Then the remote timestamp lookup has no match
