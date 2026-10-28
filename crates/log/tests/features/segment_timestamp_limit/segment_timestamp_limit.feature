Feature: Kafka timestamp lookup limits only the compressed records it reads
  Scenario Outline: A match before a large batch or outside decompression limits is returned
    Given a <codec> segment with a 10-byte record at timestamp 1000 and a 1000-byte record at 1001
    When segment timestamp lookup searches for <target> with record limit <limit>
    Then segment timestamp lookup returns offset <offset> at timestamp <timestamp>

    Examples:
      | codec        | target | limit     | offset | timestamp |
      | gzip         | 1000   | 100       | 0      | 1000      |
      | gzip         | 1001   | unlimited | 1      | 1001      |
      | uncompressed | 1001   | 100       | 1      | 1001      |

  Scenario: The compressed oversized matching record is refused
    Given a gzip segment with a 10-byte record at timestamp 1000 and a 1000-byte record at 1001
    When segment timestamp lookup searches for 1001 with record limit 100
    Then segment timestamp lookup rejects a 1007-byte record against limit 100
