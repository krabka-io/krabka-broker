Feature: tests elect leaders preferred

  Scenario: preferred election via wire returns success
    Then elect leaders preferred: preferred election via wire returns success

  Scenario: electing every partition omits the ones already on their preferred leader
    Then elect leaders preferred: electing every partition omits the ones already on their preferred leader
