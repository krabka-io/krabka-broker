Feature: SCRAM credentials, proof validation and connection lifetime

  Scenario: sasl scram sha512 happy path
    Given a SCRAM-SHA-512 listener with alice's SCRAM credential
    And alice supplies the configured SCRAM password
    When alice opens a fresh SCRAM connection and requests Metadata after verifying the server signature
    Then the SCRAM server signature verifies and Metadata succeeds

  Scenario: sasl scram sha512 wrong password closes connection
    Given a SCRAM-SHA-512 listener with alice's SCRAM credential
    And alice supplies a wrong SCRAM password
    When alice opens a fresh SCRAM connection and requests Metadata after verifying the server signature
    Then SCRAM credentials are rejected with SASL_AUTHENTICATION_FAILED
    When the refused SCRAM client reads the connection
    Then the refused SCRAM connection is closed at EOF

  Scenario: sasl scram sha256 happy path
    Given a SCRAM-SHA-256 listener with alice's SCRAM credential
    And alice supplies the configured SCRAM password
    When alice opens a fresh SCRAM connection and requests Metadata after verifying the server signature
    Then the SCRAM server signature verifies and Metadata succeeds

  Scenario: sasl scram sha256 wrong password closes connection
    Given a SCRAM-SHA-256 listener with alice's SCRAM credential
    And alice supplies a wrong SCRAM password
    When alice opens a fresh SCRAM connection and requests Metadata after verifying the server signature
    Then SCRAM credentials are rejected with SASL_AUTHENTICATION_FAILED
    When the refused SCRAM client reads the connection
    Then the refused SCRAM connection is closed at EOF

  Scenario: scram session capped by connections max reauth then closes
    Given a SCRAM-SHA-512 listener with a 300 millisecond reauthentication window
    When alice authenticates on the connection
    Then authentication succeeds
    And the session lifetime is between 200 and 300 milliseconds
    When 500 milliseconds pass before the client requests Metadata
    And the client reads the remainder of the connection
    Then the connection reaches EOF without any remaining bytes

  Scenario: scram in band reauth same principal reopens data plane
    Given alice is already authenticated using SCRAM-SHA-512 with a 30000 millisecond window
    When the connection reauthenticates as alice
    Then authentication succeeds
    And the session lifetime is between 29000 and 30000 milliseconds
    When the client requests Metadata on the same connection
    Then Metadata succeeds and names at least one broker

  Scenario: scram in band reauth with different principal closes
    Given alice is already authenticated using SCRAM-SHA-512 with a 30000 millisecond window
    When the connection reauthenticates as bob
    Then changing from alice to bob is rejected with SASL_AUTHENTICATION_FAILED
    When the client reads the remainder of the connection
    Then the connection reaches EOF without any remaining bytes

  Scenario: scram reauth after the window elapsed keeps serving round after round
    Given a SCRAM-SHA-512 listener with a 1000 millisecond reauthentication window
    When alice authenticates on the connection
    Then authentication succeeds
    When alice completes 3 expired reauthentication rounds 1050 milliseconds apart
    Then all 3 rounds succeed with lifetimes between 900 and 1000 milliseconds and reachable Metadata

  Scenario Outline: scram credential below 4096 iterations is refused in round one
    Given alice has a <mechanism> credential stretched with <iterations> iterations
    When alice performs the SCRAM authentication rounds
    Then SCRAM credential validation reports <expectation>

    Examples:
      | mechanism     | iterations | expectation        |
      | SCRAM-SHA-256 | 4095       | invalid-credentials |
      | SCRAM-SHA-512 | 4095       | invalid-credentials |
      | SCRAM-SHA-256 | 4096       | accepted           |
      | SCRAM-SHA-512 | 4096       | accepted           |

  Scenario Outline: a scram proof of the wrong length closes with no response
    Given alice has completed a valid SHA-256 SCRAM first round
    When a SCRAM client-final message with proof <proof> is submitted
    Then the SCRAM proof response is <outcome>

    Examples:
      | proof          | outcome                    |
      | empty          | absent                     |
      | 30-bytes       | absent                     |
      | 32-bytes-wrong | SASL_AUTHENTICATION_FAILED |
