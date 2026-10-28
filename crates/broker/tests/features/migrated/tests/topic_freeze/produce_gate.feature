Feature: tests topic freeze produce gate

  Scenario: a literal freeze refuses produce and the control topic still accepts
    Then topic freeze produce gate: a literal freeze refuses produce and the control topic still accepts

  Scenario: a prefix freeze refuses produce to every topic it covers
    Then topic freeze produce gate: a prefix freeze refuses produce to every topic it covers

  Scenario: a topic created after a covering prefix freeze is frozen on arrival
    Then topic freeze produce gate: a topic created after a covering prefix freeze is frozen on arrival
