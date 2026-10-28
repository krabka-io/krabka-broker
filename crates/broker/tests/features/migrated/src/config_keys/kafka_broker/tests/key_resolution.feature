Feature: Kafka listener-prefixed broker key resolution
  A listener or mechanism prefix resolves to the same base key and ConfigDef
  type. Unknown base keys remain unknown rather than acquiring a guessed type.

  Scenario Outline: Configured names retain their base key and Kafka type
    Given the broker configuration key is <configured>
    When the broker catalog resolves that configured key
    Then the configured name resolves to base key <base> with Kafka type <kind>

    Examples:
      | configured | base | kind |
      | num.io.threads | num.io.threads | INT |
      | listener.name.external.ssl.keystore.location | ssl.keystore.location | STRING |
      | listener.name.external.ssl.keystore.password | ssl.keystore.password | PASSWORD |
      | listener.name.external.plain.sasl.jaas.config | sasl.jaas.config | PASSWORD |

  Scenario Outline: Unknown configured names remain unresolved
    Given the broker configuration key is <configured>
    When the broker catalog resolves that configured key
    Then the configured name does not resolve to a broker key

    Examples:
      | configured |
      | listener.name.external.no.such.key |
      | no.such.key |
