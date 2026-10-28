Feature: Kafka 4.3.1 SASL authentication parity
  Scenario: kafka 4 3 1 client authenticates with every mechanism
    Given Kafka and Krabka pair sasl-accept enable PLAIN, both SCRAM digests and OAUTHBEARER with alice password alice-secret and principal claim sub
    When the Kafka 4.3.1 topics client lists topics on both brokers with these credentials
      | mechanism     | user  | password     | claim |
      | PLAIN         | alice | alice-secret |       |
      | SCRAM-SHA-256 | alice | alice-secret |       |
      | SCRAM-SHA-512 | alice | alice-secret |       |
      | OAUTHBEARER   | alice |              | sub   |
    Then both brokers return these authentication outcomes in credential order
      | outcome | message |
      | listed  |         |
      | listed  |         |
      | listed  |         |
      | listed  |         |

  Scenario: kafka 4 3 1 client reports kafkas refusal for bad credentials
    Given Kafka and Krabka pair sasl-refuse enable PLAIN, both SCRAM digests and OAUTHBEARER with alice password alice-secret and principal claim sub
    When the Kafka 4.3.1 topics client lists topics on both brokers with these credentials
      | mechanism     | user    | password         | claim |
      | PLAIN         | alice   | not-alice-secret |       |
      | PLAIN         | mallory | alice-secret     |       |
      | SCRAM-SHA-256 | alice   | not-alice-secret |       |
      | SCRAM-SHA-256 | mallory | alice-secret     |       |
      | SCRAM-SHA-512 | alice   | not-alice-secret |       |
      | SCRAM-SHA-512 | mallory | alice-secret     |       |
      | OAUTHBEARER   | alice   |                  | upn   |
    Then both brokers return these authentication outcomes in credential order
      | outcome | message |
      | refused | Authentication failed: Invalid username or password |
      | refused | Authentication failed: Invalid username or password |
      | refused | Authentication failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA-256 |
      | refused | Authentication failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA-256 |
      | refused | Authentication failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA-512 |
      | refused | Authentication failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA-512 |
      | refused | {"status":"invalid_token"} |
