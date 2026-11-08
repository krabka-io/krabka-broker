Feature: Kafka-compatible initial quorum configuration
  Scenario: Initial controllers accept Kafka base64 directory IDs
    Given initial controller specification 3@host:9093:AAAAAAAAAAAAAAAAAAAAAw
    When the initial controller specification is parsed
    Then the controller directory ID is UUID 3

  Scenario Outline: Undefined KRaft levels retain Kafka's feature error
    Given a dynamic formatter with requested kraft.version <level>
    When the formatter resolves dynamic quorum mode
    Then the quorum mode error is No feature:kraft.version with feature level <level>

    Examples:
      | case                 | level |
      | a negative level     | -1    |
      | one above the latest | 2     |
      | far above the latest | 9     |

  Scenario Outline: Voter endpoints use the normalized controller listener name
    Given formatter quorum mode <mode>
    And formatter controller listener name <input>
    When the formatter builds the initial voters
    Then the sole voter endpoint listener is <listener>

    Examples:
      | case                                      | mode       | input                | listener             |
      | the standalone default listener           | standalone | omitted              | CONTROLLER           |
      | a standalone explicit listener            | standalone | CONTROLLER_PLAINTEXT | CONTROLLER_PLAINTEXT |
      | an explicit voter listener is upper-cased | explicit   | controller_plaintext | CONTROLLER_PLAINTEXT |
      | the explicit voter default listener       | explicit   | omitted              | CONTROLLER           |
