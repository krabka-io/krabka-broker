Feature: DescribeConfigs identifies resources and reports Kafka configuration metadata
  Scenario Outline: Invalid or absent resource names preserve the full Kafka refusal
    Given topic orders exists and the requested configuration resource is <kind> named <name>
    When broker 1 describes the requested resource with complete configuration metadata
    Then the complete configuration resource refusal has code <code> and message <message>
    Examples:
      | kind           | name     | code | message                                                                                                                        |
      | topic          | missing  | 3    | null                                                                                                                           |
      | topic          | empty    | 17   | Topic name is invalid: the empty string is not allowed                                                                          |
      | topic          | bad/name | 17   | Topic name is invalid: 'bad/name' contains one or more characters other than ASCII alphanumerics, '.', '_' and '-'               |
      | client_metrics | empty    | 42   | Client metrics subscription name must not be empty                                                                              |
      | group          | empty    | 42   | Group name must not be empty                                                                                                   |
      | broker         | abc      | 42   | Broker id must be an integer, but it is: abc                                                                                    |
      | broker         | -1       | 42   | Unexpected broker id, expected 1 or empty string, but received -1                                                               |

  Scenario: An existing topic has describable configuration
    Given topic orders exists and the requested configuration resource is topic named orders
    When broker 1 describes the requested resource with complete configuration metadata
    Then the existing topic configuration is accepted and contains entries

  Scenario: Stable group configuration exposes exactly Kafka 4.3.1 keys
    Given group streams-1 has no configuration overrides and unstable API versions are disabled
    When the group configuration is described with stable API versions
    Then stable group configuration lists exactly the seventeen Kafka 4.3.1 keys

  Scenario: Stored keys preserve type sensitivity and mutability across broker scopes: key=num.io.threads, stored=8, value=8 (1)
    Given both default and named broker configuration resources store key num.io.threads value 8
    When the stored key is described from both broker resource scopes
    Then both stored entries disclose value 8 with sensitivity false wire type 3 and read-only false

  Scenario: Stored keys preserve type sensitivity and mutability across broker scopes: key=max.connections, stored=100, value=100 (2)
    Given both default and named broker configuration resources store key max.connections value 100
    When the stored key is described from both broker resource scopes
    Then both stored entries disclose value 100 with sensitivity false wire type 3 and read-only false

  Scenario: Stored keys preserve type sensitivity and mutability across broker scopes: key=follower.fetch.last.tiered.offset.enable, stored=true, value=true (3)
    Given both default and named broker configuration resources store key follower.fetch.last.tiered.offset.enable value true
    When the stored key is described from both broker resource scopes
    Then both stored entries disclose value true with sensitivity false wire type 1 and read-only false

  Scenario: Stored keys preserve type sensitivity and mutability across broker scopes: key=listener.name.external.ssl.keystore.location, stored=/keys/external.jks, value=/keys/external.jks (4)
    Given both default and named broker configuration resources store key listener.name.external.ssl.keystore.location value /keys/external.jks
    When the stored key is described from both broker resource scopes
    Then both stored entries disclose value /keys/external.jks with sensitivity false wire type 2 and read-only true

  Scenario: Stored keys preserve type sensitivity and mutability across broker scopes: key=listener.name.external.ssl.keystore.password, stored=hunter2, value=null (5)
    Given both default and named broker configuration resources store key listener.name.external.ssl.keystore.password value hunter2
    When the stored key is described from both broker resource scopes
    Then both stored entries disclose value null with sensitivity true wire type 9 and read-only true

  Scenario: Stored keys preserve type sensitivity and mutability across broker scopes: key=listener.name.external.plain.sasl.jaas.config, stored=secret, value=null (6)
    Given both default and named broker configuration resources store key listener.name.external.plain.sasl.jaas.config value secret
    When the stored key is described from both broker resource scopes
    Then both stored entries disclose value null with sensitivity true wire type 9 and read-only true

  Scenario: Stored keys preserve type sensitivity and mutability across broker scopes: key=plugin.custom.key, stored=x, value=null (7)
    Given both default and named broker configuration resources store key plugin.custom.key value x
    When the stored key is described from both broker resource scopes
    Then both stored entries disclose value null with sensitivity true wire type 0 and read-only true

  Scenario: A named broker includes every non-internal Kafka key and its metadata
    Given broker 1 has no dynamic configuration overrides
    When the requested configuration resource is described with synonyms and documentation
    Then every non-internal broker key is typed listed and safely disclosed with its Kafka default metadata

  Scenario: Group overrides precede the underlying broker defaults
    Given group g overrides streams.session.timeout.ms with 60000
    When the requested configuration resource is described with synonyms and documentation
    Then all Kafka group keys are listed and overrides precede exact broker default synonyms
