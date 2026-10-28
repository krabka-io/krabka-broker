Feature: Kafka UUID text and reservation contracts
  Scenario Outline: Accepted UUID strings normalize exactly as Kafka does
    Given Kafka UUID input <input>
    When the input is parsed and rendered as cluster and directory UUIDs
    Then both UUID types render the canonical text <canonical>

    Examples:
      | input | canonical |
      | AQIDBAUGBwgJCgsMDQ4PEA | AQIDBAUGBwgJCgsMDQ4PEA |
      | U-hCdTzaSPyFB3swwc7Qdw | U-hCdTzaSPyFB3swwc7Qdw |
      | AAAAAAAAAAAAAAAAAAAAAA | AAAAAAAAAAAAAAAAAAAAAA |
      | AAAAAAAAAAAAAAAAAAAAAQ | AAAAAAAAAAAAAAAAAAAAAQ |
      | _____________________x | _____________________w |
      | AAAAAAAAAAAAAAAAAAAAAB | AAAAAAAAAAAAAAAAAAAAAA |
      | AAAAAAAAAAAAAAAAAAAAAA== | AAAAAAAAAAAAAAAAAAAAAA |

  Scenario Outline: Rejected UUID strings retain Kafka diagnostics
    Given Kafka UUID input <input>
    When the input is parsed and rendered as cluster and directory UUIDs
    Then both UUID types report <error>

    Examples:
      | input | error |
      | 01020304-0506-0708-090a-0b0c0d0e0f10 | Input string with prefix `01020304-0506-0708-090a-` is too long to be decoded as a base64 UUID |
      | AAAAAAAAAAAAAAAAAAAAAAA😀 | Input string with prefix `AAAAAAAAAAAAAAAAAAAAAAA?` is too long to be decoded as a base64 UUID |
      | AAAAAAAAAAAAAAAAAAAAAAAA | Input string `AAAAAAAAAAAAAAAAAAAAAAAA` decoded as 18 bytes, which is not equal to the expected 16 bytes of a base64-encoded UUID |
      | empty | Input string `` decoded as 0 bytes, which is not equal to the expected 16 bytes of a base64-encoded UUID |
      | AAAAAA | Input string `AAAAAA` decoded as 4 bytes, which is not equal to the expected 16 bytes of a base64-encoded UUID |
      | AAAAAAAAAAAAAAAAAAAA== | Input string `AAAAAAAAAAAAAAAAAAAA==` decoded as 15 bytes, which is not equal to the expected 16 bytes of a base64-encoded UUID |
      | AAAAAAAAAAAAAAAAAAAAA | Input string `AAAAAAAAAAAAAAAAAAAAA` is not base64url: Invalid input length: 21 |
      | AAAAAAAAAAAAAAAAAAAA+A | Input string `AAAAAAAAAAAAAAAAAAAA+A` is not base64url: Invalid symbol 43, offset 20. |
      | AAAAAAAAAAAAAAAAAAAAAA= | Input string `AAAAAAAAAAAAAAAAAAAAAA=` is not base64url: Invalid padding |
      | AAAAAAAAAAAAAAAAAAAAAé | Input string `AAAAAAAAAAAAAAAAAAAAAé` is not base64url: Invalid symbol 195, offset 21. |

  Scenario: UUID files use Kafka JSON strings rather than hyphenated UUIDs
    Given cluster UUID bytes 0102030405060708090a0b0c0d0e0f10
    When cluster UUID JSON is saved and cluster and directory UUID strings are loaded
    Then JSON preserves the Kafka UUID string and rejects hyphenated directory UUIDs

  Scenario Outline: Cluster and directory UUID reservations follow their Kafka domains
    Given a UUID with high word <high> and low word <low>
    When cluster and directory UUID reservation rules are evaluated
    Then cluster reservation is <cluster> and directory reservation is <directory>

    Examples:
      | high | low                  | cluster | directory |
      | 0    | 0                    | true    | true      |
      | 0    | 1                    | true    | true      |
      | 0    | 2                    | false   | true      |
      | 0    | 99                   | false   | true      |
      | 0    | 100                  | false   | false     |
      | 0    | 18446744073709551615 | false   | true      |
      | 1    | 0                    | false   | false     |
      | 1    | 1                    | false   | false     |

  Scenario: Random UUIDs avoid Kafka's reserved identities and command-line ambiguity
    Given random UUID sample sizes 5000 plain and 500 for each wrapper
    When the random UUID samples are generated
    Then all generated UUIDs avoid their reserved values and leading dashes

  Scenario: Every leading byte is classified consistently with Kafka base64
    Given UUIDs spanning every possible leading byte
    When their leading-dash classification and base64 encodings are computed
    Then the leading-dash classifier agrees with Kafka encoding for every byte
