Feature: tests alter replica log dirs cordoned

  Scenario: a cordoned dir is flagged and refuses replica moves
    Then alter replica log dirs cordoned: a cordoned dir is flagged and refuses replica moves

  Scenario: an unknown cordoned dir is refused
    Then alter replica log dirs cordoned: an unknown cordoned dir is refused
