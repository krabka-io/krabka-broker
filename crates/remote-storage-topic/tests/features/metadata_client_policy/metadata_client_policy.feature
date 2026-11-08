Feature: Kafka metadata event-log client configuration
  Scenario: Metadata client defaults match Kafka's remote log metadata policy
    Given metadata bootstrap address 127.0.0.1:9092
    When metadata log defaults are constructed and validated
    Then metadata client defaults match Kafka's topic and transport policy

  Scenario: Default transport quantities reach the Kafka wire without rescaling
    Given metadata bootstrap address 127.0.0.1:9092
    When metadata log defaults are constructed and validated
    Then metadata transport quantities retain their exact Kafka wire images

  Scenario: Operators can choose a valid custom metadata transport policy
    Given a default metadata client targeting 127.0.0.1:9092
    And the metadata client has a custom transport policy
      | setting              | value    |
      | topic_create_timeout | 45000ms  |
      | fetch_max_wait       | 750ms    |
      | fetch_max_bytes      | 2097152B |
      | fetch_retry_backoff  | 300ms    |
      | event_queue_capacity | 2048     |
    When metadata client configuration is validated
    Then custom metadata transport settings remain valid and unchanged

  Scenario Outline: Invalid wire magnitudes are refused before connecting
    Given a default metadata client targeting 127.0.0.1:9092
    And metadata transport field <field> equals <value>
    When metadata client configuration is validated
    Then metadata transport validation refuses field <field>

    Examples:
      | case                                 | field                | value        |
      | a zero creation timeout              | topic_create_timeout | 0            |
      | a sub-millisecond creation timeout   | topic_create_timeout | 500us        |
      | an infinite creation timeout         | topic_create_timeout | infinite     |
      | a creation timeout past int32 millis | topic_create_timeout | 2147483648ms |
      | a zero fetch wait                    | fetch_max_wait       | 0            |
      | a sub-millisecond fetch wait         | fetch_max_wait       | 500us        |
      | an infinite fetch wait               | fetch_max_wait       | infinite     |
      | a fetch wait past int32 millis       | fetch_max_wait       | 2147483648ms |
      | a zero fetch size                    | fetch_max_bytes      | 0B           |
      | a fractional fetch size              | fetch_max_bytes      | 0.5B         |
      | an infinite fetch size               | fetch_max_bytes      | infinite     |
      | a fetch size past int32 bytes        | fetch_max_bytes      | 2147483648B  |
      | a zero retry backoff                 | fetch_retry_backoff  | 0            |
      | an infinite retry backoff            | fetch_retry_backoff  | infinite     |

  Scenario Outline: Minimum ISR must be positive unless the cluster default is retained
    Given a default metadata client targeting 127.0.0.1:9092
    And metadata min ISR selection <minimum>
    When metadata client configuration is validated
    Then the min ISR policy is <decision>

    Examples:
      | case                    | minimum         | decision |
      | the smallest ISR        | 1               | accepted |
      | a larger ISR            | 2               | accepted |
      | a zero ISR              | 0               | rejected |
      | a negative ISR          | -1              | rejected |
      | the cluster default ISR | cluster-default | accepted |

  Scenario Outline: Event delivery capacity cannot be zero
    Given metadata queue capacity input <capacity>
    When the metadata event queue capacity is constructed
    Then the metadata event queue capacity is <decision>

    Examples:
      | case                  | capacity | decision |
      | a zero capacity       | 0        | rejected |
      | the smallest capacity | 1        | one      |

  Scenario: Client resource bounds survive metadata client configuration
    Given a default metadata client targeting 127.0.0.1:9092
    And metadata client resource policy has dispatch capacity 7 and frame size 32768 bytes
    When metadata client configuration is validated
    Then the metadata client retains its dispatch and frame limits

  Scenario: Authenticated metadata clients retain their credentials
    Given a default metadata client targeting 127.0.0.1:9092
    And metadata client SASL_PLAINTEXT credentials u and p
    When metadata client configuration is validated
    Then the metadata client retains its security configuration
