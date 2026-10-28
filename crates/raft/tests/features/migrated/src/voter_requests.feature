Feature: src voter requests

  Scenario: tests: replica key names the directory in kafka form
    Then voter requests tests: replica key names the directory in kafka form

  Scenario Outline: tests: add voter checks run in kafka order
    Then voter requests tests: add voter checks run in kafka order for <example>

    Examples:
      | example                  |
      | foreign cluster follower |
      | follower                 |
      | invalid follower         |
      | negative voter           |
      | zero directory           |
      | no listeners             |
      | no controller listener   |
      | this cluster             |
      | no cluster id            |

  Scenario Outline: tests: remove voter checks run in kafka order
    Then voter requests tests: remove voter checks run in kafka order for <example>

    Examples:
      | example         |
      | foreign cluster |
      | follower        |
      | negative voter  |
      | zero directory  |
      | valid voter     |

  Scenario Outline: tests: update voter checks run in kafka order
    Then voter requests tests: update voter checks run in kafka order for <example>

    Examples:
      | example                |
      | foreign cluster        |
      | epoch below            |
      | epoch above            |
      | follower               |
      | zero directory         |
      | negative kraft version |
      | inverted kraft version |
      | no controller listener |
      | valid update           |

  Scenario Outline: tests: reconfiguration outcomes get kafka codes
    Then voter requests tests: reconfiguration outcomes get kafka codes for <example>

    Examples:
      | example         |
      | committed       |
      | not leader      |
      | lagging         |
      | pending change  |
      | duplicate voter |
      | unknown voter   |
      | rejected change |
