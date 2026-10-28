Feature: Reading Kafka configuration tool output
  Scenario: Resource configuration omits provenance annotations
    Given the configuration tool printed
      """
      Dynamic configs for group 'krabka-grp' are:
        consumer.session.timeout.ms=50000 sensitive=false synonyms={DYNAMIC_GROUP_CONFIG:consumer.session.timeout.ms=50000}
      """
    When the described configuration and quota entities are read
    Then the described configuration contains exactly
      | key                         | value |
      | consumer.session.timeout.ms | 50000 |

  Scenario: Quota configuration yields the same key value representation
    Given the configuration tool printed
      """
      Quota configs for client-id 'krabka-client' are consumer_byte_rate=1024.0, producer_byte_rate=2048.0
      """
    When the described configuration and quota entities are read
    Then the described configuration contains exactly
      | key                | value  |
      | consumer_byte_rate | 1024.0 |
      | producer_byte_rate | 2048.0 |

  Scenario: A tuple quota entity keeps both identity components
    Given the configuration tool printed
      """
      Quota configs for user-principal 'u', client-id 'c' are request_percentage=42.0
      """
    When the described configuration and quota entities are read
    Then the quota entity retains both user u and client c
    And the described configuration contains exactly
      | key                | value |
      | request_percentage | 42.0  |

  Scenario: A dynamic topic header contains no configuration entries
    Given the configuration tool printed
      """
      Dynamic configs for topic 't' are:
      """
    When the described configuration and quota entities are read
    Then no configuration values or quota entities are invented

  Scenario: An all-configs client-metrics header contains no configuration entries
    Given the configuration tool printed
      """
      All configs for client-metrics 'm' are:
      """
    When the described configuration and quota entities are read
    Then no configuration values or quota entities are invented

  Scenario: Empty stdout contains no configuration entries
    Given the configuration tool printed
      """

      """
    When the described configuration and quota entities are read
    Then no configuration values or quota entities are invented
