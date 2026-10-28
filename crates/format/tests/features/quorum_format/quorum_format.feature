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
      | level |
      | -1    |
      | 2     |
      | 9     |

  Scenario Outline: Voter endpoints use the normalized controller listener name
    Given formatter quorum mode <mode>
    And formatter controller listener name <input>
    When the formatter builds the initial voters
    Then the sole voter endpoint listener is <listener>

    Examples:
      | mode       | input                | listener             |
      | standalone | omitted              | CONTROLLER           |
      | standalone | CONTROLLER_PLAINTEXT | CONTROLLER_PLAINTEXT |
      | explicit   | controller_plaintext | CONTROLLER_PLAINTEXT |
      | explicit   | omitted              | CONTROLLER           |
