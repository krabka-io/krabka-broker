Feature: SSL listener store changes follow Kafka engine-factory validation
  The named renewed-keystore, renewed-private-key and renewed-certificate
  resources contain freshly generated localhost PEM key material.

  Background:
    Given a current SSL listener and a renewed localhost PEM identity

  Scenario: A PEM file uses the default keystore type
    Given the SSL listener reconfiguration supplies these properties:
      | key | value |
      | ssl.keystore.location | renewed-keystore |
    When the listener validates the SSL store reconfiguration
    Then the SSL store reconfiguration is accepted

  Scenario: An explicit PEM file type is accepted
    Given the SSL listener reconfiguration supplies these properties:
      | key | value |
      | ssl.keystore.type | PEM |
      | ssl.keystore.location | renewed-keystore |
    When the listener validates the SSL store reconfiguration
    Then the SSL store reconfiguration is accepted

  Scenario: Inline private key and certificate chain form a complete identity
    Given the SSL listener reconfiguration supplies these properties:
      | key | value |
      | ssl.keystore.key | renewed-private-key |
      | ssl.keystore.certificate.chain | renewed-certificate |
    When the listener validates the SSL store reconfiguration
    Then the SSL store reconfiguration is accepted

  Scenario: A JKS keystore is unsupported
    Given the SSL listener reconfiguration supplies these properties:
      | key | value |
      | ssl.keystore.type | JKS |
      | ssl.keystore.location | renewed-keystore |
    When the listener validates the SSL store reconfiguration
    Then the SSL store update is refused with code 42 and exact message Invalid value org.apache.kafka.common.config.ConfigException: Validation of dynamic config update of SSLFactory failed: org.apache.kafka.common.errors.InvalidConfigurationException: krabka loads PEM key stores only, but the key store type is JKS. for configuration Invalid dynamic configuration

  Scenario: PEM keystore passwords are forbidden
    Given the SSL listener reconfiguration supplies these properties:
      | key | value |
      | ssl.keystore.location | renewed-keystore |
      | ssl.keystore.password | secret |
    When the listener validates the SSL store reconfiguration
    Then the SSL store update is refused with code 42 and exact message Invalid value org.apache.kafka.common.config.ConfigException: Validation of dynamic config update of SSLFactory failed: org.apache.kafka.common.errors.InvalidConfigurationException: SSL key store password cannot be specified with PEM format, only key password may be specified for configuration Invalid dynamic configuration

  Scenario: An inline key requires its certificate chain
    Given the SSL listener reconfiguration supplies these properties:
      | key | value |
      | ssl.keystore.key | renewed-private-key |
    When the listener validates the SSL store reconfiguration
    Then the SSL store update is refused with code 42 and exact message Invalid value org.apache.kafka.common.config.ConfigException: Validation of dynamic config update of SSLFactory failed: org.apache.kafka.common.errors.InvalidConfigurationException: SSL private key is specified, but certificate chain is not specified. for configuration Invalid dynamic configuration

  Scenario: An inline certificate chain requires its private key
    Given the SSL listener reconfiguration supplies these properties:
      | key | value |
      | ssl.keystore.certificate.chain | renewed-certificate |
    When the listener validates the SSL store reconfiguration
    Then the SSL store update is refused with code 42 and exact message Invalid value org.apache.kafka.common.config.ConfigException: Validation of dynamic config update of SSLFactory failed: org.apache.kafka.common.errors.InvalidConfigurationException: SSL certificate chain is specified, but private key is not specified for configuration Invalid dynamic configuration

  Scenario: Encrypted private keys are unsupported
    Given the SSL listener reconfiguration supplies these properties:
      | key | value |
      | ssl.keystore.location | renewed-keystore |
      | ssl.key.password | secret |
    When the listener validates the SSL store reconfiguration
    Then the SSL store update is refused with code 42 and exact message Invalid value org.apache.kafka.common.config.ConfigException: Validation of dynamic config update of SSLFactory failed: org.apache.kafka.common.errors.InvalidConfigurationException: krabka cannot decrypt an encrypted PEM private key; give the key unencrypted and leave ssl.key.password unset. for configuration Invalid dynamic configuration

  Scenario: Inline trust certificates need no store password
    Given the SSL listener reconfiguration supplies these properties:
      | key | value |
      | ssl.truststore.certificates | renewed-certificate |
    When the listener validates the SSL store reconfiguration
    Then the SSL store reconfiguration is accepted

  Scenario: PEM truststore passwords are forbidden
    Given the SSL listener reconfiguration supplies these properties:
      | key | value |
      | ssl.truststore.certificates | renewed-certificate |
      | ssl.truststore.password | secret |
    When the listener validates the SSL store reconfiguration
    Then the SSL store update is refused with code 42 and exact message Invalid value org.apache.kafka.common.config.ConfigException: Validation of dynamic config update of SSLFactory failed: org.apache.kafka.common.errors.InvalidConfigurationException: SSL trust store password cannot be specified for PEM format. for configuration Invalid dynamic configuration

  Scenario: PKCS12 truststores are unsupported
    Given the SSL listener reconfiguration supplies these properties:
      | key | value |
      | ssl.truststore.type | PKCS12 |
      | ssl.truststore.location | renewed-keystore |
    When the listener validates the SSL store reconfiguration
    Then the SSL store update is refused with code 42 and exact message Invalid value org.apache.kafka.common.config.ConfigException: Validation of dynamic config update of SSLFactory failed: org.apache.kafka.common.errors.InvalidConfigurationException: krabka loads PEM trust stores only, but the trust store type is PKCS12. for configuration Invalid dynamic configuration
