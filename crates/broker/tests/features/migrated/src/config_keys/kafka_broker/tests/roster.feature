Feature: Kafka released broker configuration catalog
  The catalog preserves Kafka's complete ConfigDef shape, binary-search order,
  dynamic scope, and exact per-broker security and listener key roster.

  Scenario: Binary-search lookup receives sorted and unique broker key names
    Given the released Kafka 4.3.1 broker configuration catalog
    When the released broker configuration roster is inspected
    Then the broker key roster is sorted and contains no duplicate names

  Scenario: The released broker catalog retains Kafka's ConfigDef dimensions
    Given the released Kafka 4.3.1 broker configuration catalog
    When the released broker configuration roster is inspected
    Then the broker catalog has 360 keys, 20 internal keys, and 99 dynamic keys

  Scenario: Per-broker-only scope follows Kafka's security and listener configuration policy
    Given the released Kafka 4.3.1 broker configuration catalog
    When the released broker configuration roster is inspected
    Then the ordered per-broker-only keys are exactly:
      | key |
      | cordoned.log.dirs |
      | listener.security.protocol.map |
      | listeners |
      | principal.builder.class |
      | sasl.enabled.mechanisms |
      | sasl.jaas.config |
      | sasl.kerberos.kinit.cmd |
      | sasl.kerberos.min.time.before.relogin |
      | sasl.kerberos.principal.to.local.rules |
      | sasl.kerberos.service.name |
      | sasl.kerberos.ticket.renew.jitter |
      | sasl.kerberos.ticket.renew.window.factor |
      | sasl.login.refresh.buffer.seconds |
      | sasl.login.refresh.min.period.seconds |
      | sasl.login.refresh.window.factor |
      | sasl.login.refresh.window.jitter |
      | sasl.mechanism.inter.broker.protocol |
      | ssl.cipher.suites |
      | ssl.client.auth |
      | ssl.enabled.protocols |
      | ssl.endpoint.identification.algorithm |
      | ssl.engine.factory.class |
      | ssl.key.password |
      | ssl.keymanager.algorithm |
      | ssl.keystore.certificate.chain |
      | ssl.keystore.key |
      | ssl.keystore.location |
      | ssl.keystore.password |
      | ssl.keystore.type |
      | ssl.protocol |
      | ssl.provider |
      | ssl.secure.random.implementation |
      | ssl.trustmanager.algorithm |
      | ssl.truststore.certificates |
      | ssl.truststore.location |
      | ssl.truststore.password |
      | ssl.truststore.type |

  Scenario Outline: Startup-only broker keys cannot be changed dynamically
    Given the broker configuration key is <key>
    When the broker catalog looks up that key's dynamic mode
    Then that broker key is startup-only and read-only to dynamic alterations

    Examples:
      | key |
      | auto.leader.rebalance.enable |
      | queued.max.requests |
      | log.cleaner.enable |
      | authorizer.class.name |
      | offsets.topic.replication.factor |
      | log.dirs |
      | node.id |
      | unstable.api.versions.enable |
