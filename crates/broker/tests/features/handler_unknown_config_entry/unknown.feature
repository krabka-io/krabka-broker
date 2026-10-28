Feature: DescribeConfigs protects unregistered plugin configuration values
  Scenario: An unknown configuration entry has no public type or value
    Given an unregistered plugin key plugin.custom.key has dynamic default value x
    When DescribeConfigs renders the key with synonyms and documentation requested
    Then the unknown key and its synonym are sensitive untyped read-only and withhold their values
