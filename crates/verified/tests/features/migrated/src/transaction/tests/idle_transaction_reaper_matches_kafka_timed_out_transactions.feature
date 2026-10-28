Feature: src transaction tests idle transaction reaper matches kafka timed out transactions

  Scenario: idle transaction reaper matches kafka timed out transactions
    Then transaction tests idle transaction reaper matches kafka timed out transactions: idle transaction reaper matches kafka timed out transactions

  Scenario: producer identity boundary table
    Then transaction tests idle transaction reaper matches kafka timed out transactions: producer identity boundary table

  Scenario: init producer id identity bumps retries or fences
    Then transaction tests idle transaction reaper matches kafka timed out transactions: init producer id identity bumps retries or fences

  Scenario Outline: completion requires the prepared identity and state
    Then transaction tests idle transaction reaper matches kafka timed out transactions: completion requires the prepared identity and state for <example>

    Examples:
      | example        |
      | proceeds       |
      | stale identity |
      | wrong state    |

  Scenario: only the intended completion is idempotent
    Then transaction tests idle transaction reaper matches kafka timed out transactions: only the intended completion is idempotent
