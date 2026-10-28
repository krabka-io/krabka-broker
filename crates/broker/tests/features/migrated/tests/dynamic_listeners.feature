Feature: tests dynamic listeners

  Scenario: an added listener serves until it is removed
    Then dynamic listeners: an added listener serves until it is removed

  Scenario: an added sasl listener offers its own mechanisms
    Then dynamic listeners: an added sasl listener offers its own mechanisms

  Scenario: a stored listener opens after a restart
    Then dynamic listeners: a stored listener opens after a restart

  Scenario: a listener change kafka refuses is refused
    Then dynamic listeners: a listener change kafka refuses is refused
