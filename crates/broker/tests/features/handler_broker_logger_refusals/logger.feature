Feature: Broker logger alterations validate node logger operation and level before changing the live filter
  Background:
    Given node 3 has root and krabka_broker loggers at INFO

  Scenario: Refused logger alterations preserve Kafka diagnostics and the current level: node=not-a-number, logger=krabka_broker, operation=0 (1)
    Given the logger alteration names node not-a-number logger krabka_broker operation 0 value DEBUG
    When the live logger controller applies the alteration
    Then the logger alteration returns code 42 message Node id must be an integer, but it is: not-a-number and retains INFO

  Scenario: Refused logger alterations preserve Kafka diagnostics and the current level: node=4, logger=krabka_broker, operation=0 (2)
    Given the logger alteration names node 4 logger krabka_broker operation 0 value DEBUG
    When the live logger controller applies the alteration
    Then the logger alteration returns code 42 message Unexpected node id. Expected 3, but received 4 and retains INFO

  Scenario: Refused logger alterations preserve Kafka diagnostics and the current level: node=3, logger=no_such_target, operation=0 (3)
    Given the logger alteration names node 3 logger no_such_target operation 0 value DEBUG
    When the live logger controller applies the alteration
    Then the logger alteration returns code 40 message Logger no_such_target does not exist! and retains INFO

  Scenario: Refused logger alterations preserve Kafka diagnostics and the current level: node=3, logger=krabka_broker, operation=0 (4)
    Given the logger alteration names node 3 logger krabka_broker operation 0 value VERBOSE
    When the live logger controller applies the alteration
    Then the logger alteration returns code 40 message Cannot set the log level of krabka_broker to VERBOSE as it is not a supported log level. Valid log levels are DEBUG, ERROR, FATAL, INFO, TRACE, WARN and retains INFO

  Scenario: Refused logger alterations preserve Kafka diagnostics and the current level: node=3, logger=krabka_broker, operation=0 (5)
    Given the logger alteration names node 3 logger krabka_broker operation 0 value null
    When the live logger controller applies the alteration
    Then the logger alteration returns code 40 message Cannot set the log level of krabka_broker to null as it is not a supported log level. Valid log levels are DEBUG, ERROR, FATAL, INFO, TRACE, WARN and retains INFO

  Scenario: Refused logger alterations preserve Kafka diagnostics and the current level: node=3, logger=root, operation=1 (6)
    Given the logger alteration names node 3 logger root operation 1 value null
    When the live logger controller applies the alteration
    Then the logger alteration returns code 42 message Removing the log level of the root logger is not allowed and retains INFO

  Scenario: Refused logger alterations preserve Kafka diagnostics and the current level: node=3, logger=krabka_broker, operation=2 (7)
    Given the logger alteration names node 3 logger krabka_broker operation 2 value DEBUG
    When the live logger controller applies the alteration
    Then the logger alteration returns code 42 message APPEND operation is not allowed for the BROKER_LOGGER resource and retains INFO

  Scenario: Refused logger alterations preserve Kafka diagnostics and the current level: node=3, logger=krabka_broker, operation=3 (8)
    Given the logger alteration names node 3 logger krabka_broker operation 3 value DEBUG
    When the live logger controller applies the alteration
    Then the logger alteration returns code 42 message SUBTRACT operation is not allowed for the BROKER_LOGGER resource and retains INFO

  Scenario: Refused logger alterations preserve Kafka diagnostics and the current level: node=3, logger=krabka_broker, operation=9 (9)
    Given the logger alteration names node 3 logger krabka_broker operation 9 value DEBUG
    When the live logger controller applies the alteration
    Then the logger alteration returns code 42 message Unknown operation type 9 is not allowed for the BROKER_LOGGER resource and retains INFO

