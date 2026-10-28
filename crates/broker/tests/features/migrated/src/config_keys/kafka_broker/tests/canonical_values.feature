Feature: Kafka broker ConfigDef value canonicalization
  Dynamic broker values use Kafka's type parsers, bounds, list validation,
  case normalization, trimming, and exact ConfigException messages.

  Scenario: num.io.threads canonicalizes " 16 "
    Given the broker configuration key is num.io.threads
    And the quoted broker configuration value is " 16 "
    When Kafka ConfigDef canonicalization validates the broker value
    Then the quoted canonical broker value is "16"

  Scenario: num.io.threads refuses "abc"
    Given the broker configuration key is num.io.threads
    And the quoted broker configuration value is "abc"
    When Kafka ConfigDef canonicalization validates the broker value
    Then broker value validation fails with Kafka message Invalid value abc for configuration num.io.threads: Not a number of type INT

  Scenario: num.io.threads refuses "0"
    Given the broker configuration key is num.io.threads
    And the quoted broker configuration value is "0"
    When Kafka ConfigDef canonicalization validates the broker value
    Then broker value validation fails with Kafka message Invalid value 0 for configuration num.io.threads: Value must be at least 1

  Scenario: num.io.threads refuses "2147483648"
    Given the broker configuration key is num.io.threads
    And the quoted broker configuration value is "2147483648"
    When Kafka ConfigDef canonicalization validates the broker value
    Then broker value validation fails with Kafka message Invalid value 2147483648 for configuration num.io.threads: Not a number of type INT

  Scenario: max.connections refuses "-5"
    Given the broker configuration key is max.connections
    And the quoted broker configuration value is "-5"
    When Kafka ConfigDef canonicalization validates the broker value
    Then broker value validation fails with Kafka message Invalid value -5 for configuration max.connections: Value must be at least 0

  Scenario: log.cleaner.threads canonicalizes "0"
    Given the broker configuration key is log.cleaner.threads
    And the quoted broker configuration value is "0"
    When Kafka ConfigDef canonicalization validates the broker value
    Then the quoted canonical broker value is "0"

  Scenario: log.cleaner.threads refuses "-1"
    Given the broker configuration key is log.cleaner.threads
    And the quoted broker configuration value is "-1"
    When Kafka ConfigDef canonicalization validates the broker value
    Then broker value validation fails with Kafka message Invalid value -1 for configuration log.cleaner.threads: Value must be at least 0

  Scenario: log.cleaner.io.buffer.load.factor canonicalizes "0.75"
    Given the broker configuration key is log.cleaner.io.buffer.load.factor
    And the quoted broker configuration value is "0.75"
    When Kafka ConfigDef canonicalization validates the broker value
    Then the quoted canonical broker value is "0.75"

  Scenario: log.cleaner.io.buffer.load.factor refuses "x"
    Given the broker configuration key is log.cleaner.io.buffer.load.factor
    And the quoted broker configuration value is "x"
    When Kafka ConfigDef canonicalization validates the broker value
    Then broker value validation fails with Kafka message Invalid value x for configuration log.cleaner.io.buffer.load.factor: Not a number of type DOUBLE

  Scenario: sasl.login.refresh.buffer.seconds canonicalizes "300"
    Given the broker configuration key is sasl.login.refresh.buffer.seconds
    And the quoted broker configuration value is "300"
    When Kafka ConfigDef canonicalization validates the broker value
    Then the quoted canonical broker value is "300"

  Scenario: sasl.login.refresh.buffer.seconds refuses "40000"
    Given the broker configuration key is sasl.login.refresh.buffer.seconds
    And the quoted broker configuration value is "40000"
    When Kafka ConfigDef canonicalization validates the broker value
    Then broker value validation fails with Kafka message Invalid value 40000 for configuration sasl.login.refresh.buffer.seconds: Not a number of type SHORT

  Scenario: follower.fetch.last.tiered.offset.enable canonicalizes "TRUE"
    Given the broker configuration key is follower.fetch.last.tiered.offset.enable
    And the quoted broker configuration value is "TRUE"
    When Kafka ConfigDef canonicalization validates the broker value
    Then the quoted canonical broker value is "true"

  Scenario: follower.fetch.last.tiered.offset.enable refuses "yes"
    Given the broker configuration key is follower.fetch.last.tiered.offset.enable
    And the quoted broker configuration value is "yes"
    When Kafka ConfigDef canonicalization validates the broker value
    Then broker value validation fails with Kafka message Invalid value yes for configuration follower.fetch.last.tiered.offset.enable: Expected value to be either true or false

  Scenario: metric.reporters canonicalizes "a, b"
    Given the broker configuration key is metric.reporters
    And the quoted broker configuration value is "a, b"
    When Kafka ConfigDef canonicalization validates the broker value
    Then the quoted canonical broker value is "a,b"

  Scenario: metric.reporters canonicalizes ""
    Given the broker configuration key is metric.reporters
    And the quoted broker configuration value is ""
    When Kafka ConfigDef canonicalization validates the broker value
    Then the quoted canonical broker value is ""

  Scenario: metric.reporters refuses "a,a"
    Given the broker configuration key is metric.reporters
    And the quoted broker configuration value is "a,a"
    When Kafka ConfigDef canonicalization validates the broker value
    Then broker value validation fails with Kafka message Configuration 'metric.reporters' values must not be duplicated.

  Scenario: listeners refuses " "
    Given the broker configuration key is listeners
    And the quoted broker configuration value is " "
    When Kafka ConfigDef canonicalization validates the broker value
    Then broker value validation fails with Kafka message Configuration 'listeners' must not be empty. Valid values include: any non-empty value

  Scenario: ssl.client.auth refuses "maybe"
    Given the broker configuration key is ssl.client.auth
    And the quoted broker configuration value is "maybe"
    When Kafka ConfigDef canonicalization validates the broker value
    Then broker value validation fails with Kafka message Invalid value maybe for configuration ssl.client.auth: String must be one of: required, requested, none

  Scenario: ssl.client.auth canonicalizes " required "
    Given the broker configuration key is ssl.client.auth
    And the quoted broker configuration value is " required "
    When Kafka ConfigDef canonicalization validates the broker value
    Then the quoted canonical broker value is "required"

  Scenario: ssl.keystore.password canonicalizes " secret "
    Given the broker configuration key is ssl.keystore.password
    And the quoted broker configuration value is " secret "
    When Kafka ConfigDef canonicalization validates the broker value
    Then the quoted canonical broker value is "secret"

  Scenario: principal.builder.class canonicalizes "no.such.Class"
    Given the broker configuration key is principal.builder.class
    And the quoted broker configuration value is "no.such.Class"
    When Kafka ConfigDef canonicalization validates the broker value
    Then the quoted canonical broker value is "no.such.Class"

  Scenario: group.coordinator.cached.buffer.max.bytes refuses "1"
    Given the broker configuration key is group.coordinator.cached.buffer.max.bytes
    And the quoted broker configuration value is "1"
    When Kafka ConfigDef canonicalization validates the broker value
    Then broker value validation fails with Kafka message Invalid value 1 for configuration group.coordinator.cached.buffer.max.bytes: Value must be at least 524288
