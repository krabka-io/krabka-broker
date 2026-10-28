Feature: Kafka broker configuration descriptors
  DescribeConfigs hides password values and reports the correct read-only
  marker and protocol type for each key's actual catalog definition.

  Scenario Outline: Broker key descriptors preserve sensitivity mode and wire type
    Given the broker configuration key is <key>
    When DescribeConfigs derives that broker key's public descriptor
    Then the broker descriptor is sensitive <sensitive>, read-only <read_only>, with wire type <wire_type>

    Examples:
      | key | sensitive | read_only | wire_type |
      | ssl.keystore.password | true | false | 9 |
      | log.dirs | false | true | 7 |
