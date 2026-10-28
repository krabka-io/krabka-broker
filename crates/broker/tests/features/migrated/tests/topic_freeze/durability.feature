Feature: Freeze registry survives controller recovery
  Scenario: a freeze survives a controller restart
    Given a controller has durably frozen orders after one acknowledged record
    When the controller restarts from the same data directory
    Then the freeze registry still contains the literal orders scope
    When the client writes to orders and its control after the restart
    Then orders remains refused at log end one while its control accepts a record
