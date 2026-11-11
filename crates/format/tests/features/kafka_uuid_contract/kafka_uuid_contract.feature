Feature: Kafka UUID text and reservation contracts
  Scenario Outline: Accepted UUID strings normalize exactly as Kafka does
    Given Kafka UUID input <input>
    When the input is parsed and rendered as cluster and directory UUIDs
    Then both UUID types render the canonical text <canonical>

    Examples:
      | case                                           | input | canonical |
      | a canonical identifier                         | AQIDBAUGBwgJCgsMDQ4PEA | AQIDBAUGBwgJCgsMDQ4PEA |
      | url-safe characters                            | U-hCdTzaSPyFB3swwc7Qdw | U-hCdTzaSPyFB3swwc7Qdw |
      | the all-zero UUID                              | AAAAAAAAAAAAAAAAAAAAAA | AAAAAAAAAAAAAAAAAAAAAA |
      | the smallest nonzero UUID                      | AAAAAAAAAAAAAAAAAAAAAQ | AAAAAAAAAAAAAAAAAAAAAQ |
      | trailing bits are cleared from the last symbol | _____________________x | _____________________w |
      | a nonzero trailing bit                         | AAAAAAAAAAAAAAAAAAAAAB | AAAAAAAAAAAAAAAAAAAAAA |
      | padding is accepted                            | AAAAAAAAAAAAAAAAAAAAAA== | AAAAAAAAAAAAAAAAAAAAAA |

  Scenario Outline: Rejected UUID strings retain Kafka diagnostics
    Given Kafka UUID input <input>
    When the input is parsed and rendered as cluster and directory UUIDs
    Then both UUID types report <error>

    Examples:
      | case                                 | input | error |
      | a hyphenated UUID                    | 01020304-0506-0708-090a-0b0c0d0e0f10 | Input string with prefix `01020304-0506-0708-090a-` is too long to be decoded as a base64 UUID |
      | a multibyte character past the limit | AAAAAAAAAAAAAAAAAAAAAAA😀 | Input string with prefix `AAAAAAAAAAAAAAAAAAAAAAA?` is too long to be decoded as a base64 UUID |
      | too many base64 characters           | AAAAAAAAAAAAAAAAAAAAAAAA | Input string `AAAAAAAAAAAAAAAAAAAAAAAA` decoded as 18 bytes, which is not equal to the expected 16 bytes of a base64-encoded UUID |
      | an empty string                      | empty | Input string `` decoded as 0 bytes, which is not equal to the expected 16 bytes of a base64-encoded UUID |
      | too few base64 characters            | AAAAAA | Input string `AAAAAA` decoded as 4 bytes, which is not equal to the expected 16 bytes of a base64-encoded UUID |
      | padding that decodes too few bytes   | AAAAAAAAAAAAAAAAAAAA== | Input string `AAAAAAAAAAAAAAAAAAAA==` decoded as 15 bytes, which is not equal to the expected 16 bytes of a base64-encoded UUID |
      | an impossible base64 length          | AAAAAAAAAAAAAAAAAAAAA | Input string `AAAAAAAAAAAAAAAAAAAAA` is not base64url: Invalid input length: 21 |
      | a standard-alphabet character        | AAAAAAAAAAAAAAAAAAAA+A | Input string `AAAAAAAAAAAAAAAAAAAA+A` is not base64url: Invalid symbol 43, offset 20. |
      | incomplete padding                   | AAAAAAAAAAAAAAAAAAAAAA= | Input string `AAAAAAAAAAAAAAAAAAAAAA=` is not base64url: Invalid padding |
      | a non-ASCII character                | AAAAAAAAAAAAAAAAAAAAAé | Input string `AAAAAAAAAAAAAAAAAAAAAé` is not base64url: Invalid symbol 195, offset 21. |

  Scenario: UUID files use Kafka JSON strings rather than hyphenated UUIDs
    Given cluster UUID bytes 0102030405060708090a0b0c0d0e0f10
    When cluster UUID JSON is saved and cluster and directory UUID strings are loaded
    Then JSON preserves the Kafka UUID string and rejects hyphenated directory UUIDs

  Scenario Outline: Cluster and directory UUID reservations follow their Kafka domains
    Given a UUID with high word <high> and low word <low>
    When cluster and directory UUID reservation rules are evaluated
    Then cluster reservation is <cluster> and directory reservation is <directory>

    Examples:
      | case                                  | high | low                  | cluster | directory |
      | the zero UUID                         | 0    | 0                    | true    | true      |
      | the metadata topic UUID               | 0    | 1                    | true    | true      |
      | the first unreserved cluster UUID     | 0    | 2                    | false   | true      |
      | the last reserved directory UUID      | 0    | 99                   | false   | true      |
      | the first unreserved directory UUID   | 0    | 100                  | false   | false     |
      | the largest low word                  | 0    | 18446744073709551615 | false   | true      |
      | a nonzero high word                   | 1    | 0                    | false   | false     |
      | a nonzero high word with low word one | 1    | 1                    | false   | false     |

  Scenario Outline: Random UUIDs avoid Kafka's reserved identities and command-line ambiguity
    Given random UUID sample sizes <plain> plain and <wrappers> for each wrapper
    When the random UUID samples are generated
    Then all generated UUIDs avoid their reserved values and leading dashes

    Examples:
      | case                               | plain | wrappers |
      | a large sample across UUID domains | 5000  | 500      |
      | independently sized UUID samples   | 1     | 2        |

  Scenario: Every leading byte is classified consistently with Kafka base64
    Given UUIDs spanning every possible leading byte
    When their leading-dash classification and base64 encodings are computed
    Then the leading-dash classifier agrees with Kafka encoding for every byte
