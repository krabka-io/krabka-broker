Feature: Reading the broker's advertised API versions from JVM tool output

  Scenario: tests: reads every row kind of a single broker block
    Given the JVM API versions tool prints:
      """
      host.docker.internal:41551 (id: 1 rack: null isFenced: false) -> (
          Produce(0): 3 to 13 [usable: 13],
          ApiVersions(18): 0 to 4 [usable: 4],
          ListPartitionReassignments(46): 0 [usable: 0],
          GetTelemetrySubscriptions(71): UNSUPPORTED,
          ShareFetch(78): 1 to 2 [usable: UNSUPPORTED],
          UNKNOWN(1010): 0
      )
      """
    When the output is read as a single broker response
    Then the advertised rows in API key order are:
      | name                      | key  | minimum | maximum |
      | Produce                   | 0    | 3       | 13      |
      | ApiVersions               | 18   | 0       | 4       |
      | ListPartitionReassignments | 46   | 0       | 0       |
      | ShareFetch                | 78   | 1       | 2       |
      | UNKNOWN                   | 1010 | 0       | 0       |

  Scenario: tests: advertised keeps the version ranges
    Given the JVM API versions tool prints:
      """
      host.docker.internal:41551 (id: 1 rack: null isFenced: false) -> (
          Produce(0): 3 to 13 [usable: 13],
          ApiVersions(18): 0 to 4 [usable: 4],
          ListPartitionReassignments(46): 0 [usable: 0],
          GetTelemetrySubscriptions(71): UNSUPPORTED,
          ShareFetch(78): 1 to 2 [usable: UNSUPPORTED],
          UNKNOWN(1010): 0
      )
      """
    When the output is read as a single broker response
    And the parsed rows are projected into advertised wire ranges
    Then the advertised wire ranges are:
      | key  | minimum | maximum |
      | 0    | 3       | 13      |
      | 18   | 0       | 4       |
      | 46   | 0       | 0       |
      | 78   | 1       | 2       |
      | 1010 | 0       | 0       |

  Scenario: tests: rows come back sorted by api key
    Given the JVM API versions tool prints:
      """
      n:1 (id: 1) -> (
          Fetch(1): 0 to 1 [usable: 1],
          Produce(0): 0 [usable: 0]
      )
      """
    When the output is read as a single broker response
    Then the advertised rows in API key order are:
      | name    | key | minimum | maximum |
      | Produce | 0   | 0       | 0       |
      | Fetch   | 1   | 0       | 1       |

  Scenario: tests: rejects outputs that are not one broker
    Given the JVM API versions tool prints:
      """
      Error while executing broker api versions command
      """
    When the output is read as a single broker response
    Then parsing fails because no broker block was printed
    Given the JVM tool prints 2 broker blocks
    When the output is read as a single broker response
    Then parsing fails because 2 broker blocks were printed

  Scenario: tests: rejects a row whose range does not parse
    Given the JVM API versions tool prints:
      """
      n:1 (id: 1) -> (
          Produce(0): 3 to twelve [usable: 12]
      )
      """
    When the output is read as a single broker response
    Then parsing rejects the malformed row Produce(0): 3 to twelve [usable: 12]
