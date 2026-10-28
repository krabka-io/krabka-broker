Feature: Listener protocol maps normalize names and enforce Kafka syntax

  Scenario: empty
    Given the protocol mapping is JSON ""
    When the listener protocol mapping is parsed
    Then the listener protocol mapping is empty

  Scenario: lower case
    Given the protocol mapping is JSON "internal:sasl_ssl, PLAINTEXT:PLAINTEXT,"
    When the listener protocol mapping is parsed
    Then the normalized listener protocol mappings are:
      | name | protocol |
      | INTERNAL | SaslSsl |
      | PLAINTEXT | Plaintext |

  Scenario: later entry wins
    Given the protocol mapping is JSON "A:SSL,A:PLAINTEXT"
    When the listener protocol mapping is parsed
    Then the normalized listener protocol mappings are:
      | name | protocol |
      | A | Plaintext |

  Scenario: no colon
    Given the protocol mapping is JSON "PLAINTEXT"
    When the listener protocol mapping is parsed
    Then protocol mapping fails with Error parsing configuration property 'listener.security.protocol.map': begin 0, end -1, length 9

  Scenario: unknown protocol
    Given the protocol mapping is JSON "A:TLS"
    When the listener protocol mapping is parsed
    Then protocol mapping fails with Invalid security protocol `TLS` defined in listener.security.protocol.map

  Scenario: names meet once normalised
    Given the protocol mapping is JSON "a:SSL,A:SSL"
    When the listener protocol mapping is parsed
    Then protocol mapping fails with Duplicate key A (attempted merging values SSL and SSL)
