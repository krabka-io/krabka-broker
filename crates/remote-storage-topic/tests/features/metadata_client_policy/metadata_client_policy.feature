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
    And metadata transport field topic_create_timeout equals 45000ms
    And metadata transport field fetch_max_wait equals 750ms
    And metadata transport field fetch_max_bytes equals 2097152B
    And metadata transport field fetch_retry_backoff equals 300ms
    And metadata event delivery capacity 2048
    When metadata client configuration is validated
    Then custom metadata transport settings remain valid and unchanged

  Scenario Outline: Invalid wire magnitudes are refused before connecting
    Given a default metadata client targeting 127.0.0.1:9092
    And metadata transport field <field> equals <value>
    When metadata client configuration is validated
    Then metadata transport validation refuses field <field>

    Examples:
      | field | value |
      | topic_create_timeout | 0 |
      | topic_create_timeout | 500us |
      | topic_create_timeout | infinite |
      | topic_create_timeout | 2147483648ms |
      | fetch_max_wait | 0 |
      | fetch_max_wait | 500us |
      | fetch_max_wait | infinite |
      | fetch_max_wait | 2147483648ms |
      | fetch_max_bytes | 0B |
      | fetch_max_bytes | 0.5B |
      | fetch_max_bytes | infinite |
      | fetch_max_bytes | 2147483648B |
      | fetch_retry_backoff | 0 |
      | fetch_retry_backoff | infinite |

  Scenario Outline: Minimum ISR must be positive unless the cluster default is retained
    Given a default metadata client targeting 127.0.0.1:9092
    And metadata min ISR selection <minimum>
    When metadata client configuration is validated
    Then the min ISR policy is <decision>

    Examples:
      | minimum         | decision |
      | 1               | accepted |
      | 2               | accepted |
      | 0               | rejected |
      | -1              | rejected |
      | cluster-default | accepted |

  Scenario Outline: Event delivery capacity cannot be zero
    Given metadata queue capacity input <capacity>
    When the metadata event queue capacity is constructed
    Then the metadata event queue capacity is <decision>

    Examples:
      | capacity | decision |
      | 0        | rejected |
      | 1        | one      |

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
