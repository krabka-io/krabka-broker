Feature: Change broker logging without restarting the broker
  Scenario: broker logger alter moves the live filter and the describe agrees
    Given a broker using the production INFO filter and a capturing subscriber
    When a DEBUG event is emitted on the broker child target
    Then the DEBUG event is filtered out
    When the broker logger is set to DEBUG through IncrementalAlterConfigs
    Then the broker logger alteration succeeds
    When the local broker logger configuration is described
    Then the logger reports DEBUG while root retains INFO with dynamic sources
    When a DEBUG event is emitted on the broker child target
    Then the DEBUG event reaches the capturing subscriber
    When broker logger resources are listed
    Then the local broker logger resource is listed
    When broker logger configuration is described for node 99
    Then the foreign logger request is refused as node local
