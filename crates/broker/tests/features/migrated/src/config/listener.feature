Feature: Listener connection windows follow Kafka's per-listener overrides

  Rule: Without configuration a listener keeps Kafka's defaults

    Scenario: An unconfigured listener inherits Kafka's ten-minute idle window
      When the broker resolves the idle window of listener PLAINTEXT
      Then the window is 600000 ms

    Scenario: An unconfigured listener never asks a SASL client to re-authenticate
      When the broker resolves the re-authentication window of listener SASL_SSL
      Then the listener has no window

  Rule: A listener override wins over the broker-wide window, whatever the case of its name

    Scenario Outline: An override applies to its own listener alone
      Given the broker-wide <window> window is <broker> ms
      And listener <override> overrides the <window> window with <value> ms
      When the broker resolves the <window> window of listener <listener>
      Then the window is <expected> ms

      Examples:
        | case                                                 | window            | broker  | override | value | listener | expected |
        | the idle override heads its listener                 | idle              | 600000  | EXTERNAL | 20000 | EXTERNAL | 20000    |
        | another listener keeps the idle window               | idle              | 600000  | EXTERNAL | 20000 | INTERNAL | 600000   |
        | a lowercase idle override still matches              | idle              | 600000  | external | 20000 | EXTERNAL | 20000    |
        | the re-authentication override heads its listener    | re-authentication | 3600000 | EXTERNAL | 60000 | EXTERNAL | 60000    |
        | another listener keeps the re-authentication window  | re-authentication | 3600000 | EXTERNAL | 60000 | INTERNAL | 3600000  |
        | a lowercase re-authentication override still matches | re-authentication | 3600000 | sasl_ssl | 60000 | SASL_SSL | 60000    |

    Scenario: A re-authentication override applies without a broker-wide window
      Given listener EXTERNAL overrides the re-authentication window with 60000 ms
      When the broker resolves the re-authentication window of listener EXTERNAL
      Then the window is 60000 ms

    Scenario: Without a broker-wide window another listener has no re-authentication window
      Given listener EXTERNAL overrides the re-authentication window with 60000 ms
      When the broker resolves the re-authentication window of listener INTERNAL
      Then the listener has no window

  Rule: A window that is not positive expires nothing

    Scenario Outline: A broker-wide window of zero or less
      Given the broker-wide <window> window is <broker> ms
      When the broker resolves the <window> window of listener PLAINTEXT
      Then the listener has no window

      Examples:
        | case                                | window            | broker |
        | an idle window of zero              | idle              | 0      |
        | a negative idle window              | idle              | -1     |
        | a re-authentication window of zero  | re-authentication | 0      |
        | a negative re-authentication window | re-authentication | -1     |

    Scenario Outline: An override of zero or less turns the broker-wide window off for its listener
      Given the broker-wide <window> window is <broker> ms
      And listener PLAINTEXT overrides the <window> window with <value> ms
      When the broker resolves the <window> window of listener PLAINTEXT
      Then the listener has no window

      Examples:
        | case                                  | window            | broker  | value |
        | an idle override of zero              | idle              | 600000  | 0     |
        | a re-authentication override of zero  | re-authentication | 3600000 | 0     |
        | a negative re-authentication override | re-authentication | 3600000 | -1    |
