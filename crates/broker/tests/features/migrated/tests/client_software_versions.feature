Feature: Client identity validation and per-version authentication metrics
  # KIP-511 requires client software fields on ApiVersions v3 and later;
  # malformed fields return Kafka INVALID_REQUEST (42).

  Scenario: v3 client info validation: valid
    Given a plaintext broker with client software metrics enabled
    And client software name is "krabka-client-core"
    And client software version is "0.1.1"
    When ApiVersions version 3 is sent with that client identity
    Then ApiVersions returns error code 0
    And the advertised API list is nonempty

  Scenario: v3 client info validation: empty name
    Given a plaintext broker with client software metrics enabled
    And client software name is empty
    And client software version is "1.0.0"
    When ApiVersions version 3 is sent with that client identity
    Then ApiVersions returns error code 42
    And the advertised API list is empty

  Scenario: v3 client info validation: empty version
    Given a plaintext broker with client software metrics enabled
    And client software name is "krabka"
    And client software version is empty
    When ApiVersions version 3 is sent with that client identity
    Then ApiVersions returns error code 42

  Scenario: v3 client info validation: invalid name
    Given a plaintext broker with client software metrics enabled
    And client software name is "has space"
    And client software version is "1.0.0"
    When ApiVersions version 3 is sent with that client identity
    Then ApiVersions returns error code 42

  Scenario: v3 client info validation: leading dash version
    Given a plaintext broker with client software metrics enabled
    And client software name is "krabka"
    And client software version is "-1.0.0"
    When ApiVersions version 3 is sent with that client identity
    Then ApiVersions returns error code 42

  Scenario: pre v3 does not validate client info
    Given a plaintext broker with client software metrics enabled
    And client software name is empty
    And client software version is empty
    When ApiVersions version 0 is sent with that client identity
    Then ApiVersions returns error code 0
    And the advertised API list is nonempty

  Scenario: accepted v3 handshake bumps client software versions counter
    Given a plaintext broker with client software metrics enabled
    When these ApiVersions v3 handshakes are sent:
      | name        | version | repetitions |
      | krabka-it   | 1.0.0   | 2           |
      | krabka-it   | 1.0.1   | 1           |
      | another-lib | 9.9.9   | 1           |
    And the client software metrics are scraped
    Then the exported client software family is a counter with these samples:
      | name        | version | count |
      | krabka-it   | 1.0.0   | 2     |
      | krabka-it   | 1.0.1   | 1     |
      | another-lib | 9.9.9   | 1     |

  Scenario: rejected v3 handshake does not bump counter
    Given a plaintext broker with client software metrics enabled
    When these ApiVersions v3 handshakes are sent:
      | name         | version | repetitions |
      | valid-client | 1.0.0   | 1           |
      | bad client   | 1.0.0   | 1           |
    And the client software metrics are scraped
    Then no client software metric is labelled with bad client
    And a client software metric is labelled with valid-client

  Scenario: pre v3 handshake does not bump counter
    Given a plaintext broker with client software metrics enabled
    And client software name is empty
    And client software version is empty
    When ApiVersions version 0 is sent with that client identity
    And the client software metrics are scraped
    Then no client software counter sample has an empty software name
