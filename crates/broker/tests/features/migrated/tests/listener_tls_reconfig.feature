Feature: Dynamic SSL keystore changes preserve identity compatibility and validation semantics
  Scenario: A live listener reloads renewed rewritten and deleted dynamic PEM keystores
    Given a running SSL listener serves CN localhost with SAN localhost from its static certificate
    And candidate PEM keystores have these certificate identities
      | file | CN | SAN |
      | dynamic.pem | localhost | localhost |
    When the operator applies PEM keystore dynamic.pem to the live SSL listener
    Then the complete dynamic configuration response accepts the broker resource
    And new handshakes serve keystore dynamic.pem
    And new handshakes no longer accept the original static certificate
    When PEM keystore dynamic.pem is rewritten with a fresh CN localhost SAN localhost identity and a modification time five seconds later
    And the operator applies PEM keystore dynamic.pem to the live SSL listener
    Then the complete dynamic configuration response accepts the broker resource
    And new handshakes serve keystore dynamic.pem
    When the operator deletes the dynamic SSL keystore location
    Then the complete dynamic configuration response accepts the broker resource
    And new handshakes again serve the original static certificate

  Scenario: Incompatible keystores are refused and valid validation leaves the listener unchanged
    Given a running SSL listener serves CN localhost with SAN localhost from its static certificate
    And candidate PEM keystores have these certificate identities
      | file | CN | SAN |
      | elsewhere.pem | elsewhere | localhost |
      | narrower.pem | localhost | other.example |
    And candidate keystore file missing.pem does not exist
    When the operator validates every update then applies every update in this order
      | file | keystore_type |
      | elsewhere.pem | inherit |
      | narrower.pem | inherit |
      | elsewhere.pem | JKS |
      | missing.pem | inherit |
    Then both validation and application return these complete invalid request causes in order
      | cause |
      | org.apache.kafka.common.config.ConfigException: Keystore DistinguishedName does not match:  existing={alias=kafka, DN=CN=localhost}, new={alias=kafka, DN=CN=elsewhere} |
      | org.apache.kafka.common.config.ConfigException: Keystore SubjectAltNames do not match:  existing={alias=kafka, SAN=[[2, localhost]]}, new={alias=kafka, SAN=[[2, other.example]]} |
      | org.apache.kafka.common.errors.InvalidConfigurationException: krabka loads PEM key stores only, but the key store type is JKS. |
      | org.apache.kafka.common.errors.InvalidConfigurationException: Failed to load PEM SSL keystore {missing.pem}: No such file or directory (os error 2) |
    Given candidate PEM keystores have these certificate identities
      | file | CN | SAN |
      | renewed.pem | localhost | localhost |
    When the operator validates PEM keystore renewed.pem without applying it
    Then the complete dynamic configuration response accepts the broker resource
    And validation keeps serving the static identity and does not serve candidate renewed.pem
