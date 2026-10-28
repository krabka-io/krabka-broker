Feature: Freeze visibility through Kafka topic configuration
  Scenario: neither alter path can set or clear a freeze
    Given orders is frozen for cutover beside an unfrozen config control
    When the administrator tries both config APIs to set and clear write.freeze
    Then all four config mutations refuse the controller-managed freeze key
    When clients attempt writes to orders and its control
    Then orders remains frozen at zero and the config control accepts its first record
    When the administrator alters ordinary retention on the control topic
    Then the ordinary retention alteration succeeds

  Scenario: describe configs reports the freeze read only and names the scope
    Given literal orders and namespace tenant-a are frozen beside a config control
    When the administrator describes write.freeze for literal prefix and unfrozen topics
    Then all freeze configs are read-only and identify the matching scope
