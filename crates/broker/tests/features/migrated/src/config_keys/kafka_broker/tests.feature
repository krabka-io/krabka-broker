Feature: src config keys kafka broker tests

  Scenario: the roster has the shape of kafka 4 3 1s config def
    Then config keys kafka broker tests: the roster has the shape of kafka 4 3 1s config def

  Scenario: the per broker keys are kafkas
    Then config keys kafka broker tests: the per broker keys are kafkas

  Scenario Outline: keys kafka refuses to alter are read only
    Then config keys kafka broker tests: keys kafka refuses to alter are read only for <example>

    Examples:
      | example                          |
      | auto leader rebalance enable     |
      | queued max requests              |
      | log cleaner enable               |
      | authorizer class name            |
      | offsets topic replication factor |
      | log dirs                         |
      | node id                          |
      | unstable api versions enable     |

  Scenario: the roster is sorted by name without repeats
    Then config keys kafka broker tests: the roster is sorted by name without repeats

  Scenario: dynamic keys are parsed and range checked the way config def does
    Then config keys kafka broker tests: dynamic keys are parsed and range checked the way config def does

  Scenario Outline: a listener override resolves to the key it overrides
    Then config keys kafka broker tests: a listener override resolves to the key it overrides for <example>

    Examples:
      | example               |
      | broker config key     |
      | ssl keystore location |
      | ssl keystore password |
      | sasl jaas config      |
      | listener missing key  |
      | missing key           |

  Scenario: the row of a key states its sensitivity and mode
    Then config keys kafka broker tests: the row of a key states its sensitivity and mode
