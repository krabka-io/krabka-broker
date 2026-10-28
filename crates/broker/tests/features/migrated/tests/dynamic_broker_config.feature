Feature: Running brokers apply dynamic connection settings and report exact configuration metadata
  Scenario Outline: connection cap applies and lifts without restart
    Given a running plaintext broker has its default static connection settings
    When config <key> on resource <scope> is set to 2 with validate only false
    Then the complete broker config change response is successful for the requested resource
    And with two held data connections from default a third is eventually served false
    When config <key> is deleted from resource <scope>
    Then the complete broker config change response is successful for the requested resource
    And with two held data connections from default a third is eventually served true
    Examples:
      | key | scope |
      | max.connections | default |
      | max.connections | this-broker |
      | max.connections.per.ip | default |
      | max.connections.per.ip | this-broker |

  Scenario: per broker connection cap overrides the cluster default
    Given a running plaintext broker has its default static connection settings
    When config max.connections.per.ip on resource default is set to 2 with validate only false
    Then the complete broker config change response is successful for the requested resource
    And with two held data connections from default a third is eventually served false
    When config max.connections.per.ip on resource this-broker is set to 100 with validate only false
    Then the complete broker config change response is successful for the requested resource
    And with two held data connections from default a third is eventually served true

  Scenario: static connection cap holds without a dynamic value
    Given a plaintext broker starts with static per-IP cap 1
    When config max.connections on resource default is set to 1000 with validate only false
    Then the complete broker config change response is successful for the requested resource
    And with 1 held data connections the next is immediately served false

  Scenario Outline: invalid broker config changes are refused with kafkas errors
    Given a running plaintext broker has its default static connection settings
    When config <key> on resource <scope> is set to <value> with validate only <validate>
    Then the complete broker config change response has error 42 and message <message>
    And with 2 held data connections the next is immediately served true
    Examples:
      | scope | key | value | validate | message |
      | default | log.dirs | /tmp | true | Cannot update these configs dynamically: [log.dirs] |
      | this-broker | ssl.keystore.location | /k | true | These security configs can be dynamically updated only per-listener using the listener prefix: [ssl.keystore.location] |
      | default | listener.name.plaintext.ssl.keystore.location | /k | true | Cannot update these configs at default cluster level, broker id must be specified: [listener.name.plaintext.ssl.keystore.location] |
      | default | max.connections | -5 | true | Invalid value -5 for configuration max.connections: Value must be at least 0 |
      | this-broker | max.connections.per.ip | many | true | Invalid value many for configuration max.connections.per.ip: Not a number of type INT |
      | 7 | max.connections | 1 | true | Unexpected broker id, expected 1, but received 7 |
      | default | log.dirs | /tmp | false | Cannot update these configs dynamically: [log.dirs] |
      | this-broker | ssl.keystore.location | /k | false | These security configs can be dynamically updated only per-listener using the listener prefix: [ssl.keystore.location] |
      | default | listener.name.plaintext.ssl.keystore.location | /k | false | Cannot update these configs at default cluster level, broker id must be specified: [listener.name.plaintext.ssl.keystore.location] |
      | default | max.connections | -5 | false | Invalid value -5 for configuration max.connections: Value must be at least 0 |
      | this-broker | max.connections.per.ip | many | false | Invalid value many for configuration max.connections.per.ip: Not a number of type INT |
      | 7 | max.connections | 1 | false | Unexpected broker id, expected 1, but received 7 |

  Scenario: validate only broker change has no effect
    Given a running plaintext broker has its default static connection settings
    When config max.connections.per.ip on resource default is set to 2 with validate only true
    Then the complete broker config change response is successful for the requested resource
    And with 2 held data connections the next is immediately served true
    When the client describes config max.connections.per.ip on resource default
    Then the described config entry is absent

  Scenario: describe reports dynamic values and redacts passwords
    Given a running plaintext broker has its default static connection settings
    When broker resource this-broker receives these config mutations:
      | key | operation | value |
      | listener.name.plaintext.ssl.keystore.password | set | s3cret |
      | max.connections.per.ip | set | 50 |
    Then the complete broker config change response is successful for the requested resource
    When config max.connections on resource default is set to 500 with validate only false
    Then the complete broker config change response is successful for the requested resource
    When the client describes config listener.name.plaintext.ssl.keystore.password on resource this-broker
    Then the complete described config entry has these fields:
      | name | value | source | sensitive | kind | read_only |
      | listener.name.plaintext.ssl.keystore.password | redacted | 2 | true | 9 | true |
    When the client describes config max.connections.per.ip on resource this-broker
    Then the complete described config entry has these fields:
      | name | value | source | sensitive | kind | read_only |
      | max.connections.per.ip | 50 | 2 | false | 3 | false |
    When the client describes config max.connections on resource this-broker
    Then the complete described config entry has these fields:
      | name | value | source | sensitive | kind | read_only |
      | max.connections | 500 | 3 | false | 3 | false |
    When the client describes config max.connections on resource default
    Then the complete described config entry has these fields:
      | name | value | source | sensitive | kind | read_only |
      | max.connections | 500 | 3 | false | 3 | false |

  Scenario: per ip cap applies to the controller listener
    Given a running plaintext broker has its default static connection settings
    When config max.connections.per.ip on resource default is set to 2 with validate only false
    Then the complete broker config change response is successful for the requested resource
    And with two held controller connections from 127.0.0.2 a third is eventually served false
    When config max.connections.per.ip is deleted from resource default
    Then the complete broker config change response is successful for the requested resource
    And with two held controller connections from 127.0.0.2 a third is eventually served true

  Scenario: zero per ip cap without overrides is stored but not applied
    Given a running plaintext broker has its default static connection settings
    When config max.connections.per.ip on resource default is set to 0 with validate only false
    Then the complete broker config change response is successful for the requested resource
    When the client describes config max.connections.per.ip on resource default
    Then the described config value is 0
    And with 1 held data connections the next is immediately served true

  Scenario: per ip overrides apply and lift without restart
    Given a running plaintext broker has its default static connection settings
    When broker resource default receives these config mutations:
      | key | operation | value |
      | max.connections.per.ip | set | 0 |
      | max.connections.per.ip.overrides | set | 127.0.0.1:2 |
    Then the complete broker config change response is successful for the requested resource
    And a new data connection from 127.0.0.2 is eventually served false
    And with two held data connections from default a third is eventually served false
    When broker resource default receives these config mutations:
      | key | operation | value |
      | max.connections.per.ip | delete | none |
      | max.connections.per.ip.overrides | delete | none |
    Then the complete broker config change response is successful for the requested resource
    And two held default-source connections allow both another default-source and a 127.0.0.2 connection

  Scenario: listener cap applies and lifts without restart
    Given a running plaintext broker has its default static connection settings
    When config listener.name.plaintext.max.connections on resource default is set to 2 with validate only false
    Then the complete broker config change response is successful for the requested resource
    And with two held data connections from default a third is eventually served false
    When config listener.name.plaintext.max.connections is deleted from resource default
    Then the complete broker config change response is successful for the requested resource
    And with two held data connections from default a third is eventually served true

  Scenario Outline: creation rate holds the acceptor without restart
    Given a running plaintext broker has its default static connection settings
    When config <key> on resource default is set to 1 with validate only false
    Then the complete broker config change response is successful for the requested resource
    When three consecutive data connections are timed until the creation rate propagates
    Then serving three connections took at least 1500 milliseconds
    When config <key> is deleted from resource default
    Then the complete broker config change response is successful for the requested resource
    Examples:
      | key |
      | max.connection.creation.rate |
      | listener.name.plaintext.max.connection.creation.rate |
