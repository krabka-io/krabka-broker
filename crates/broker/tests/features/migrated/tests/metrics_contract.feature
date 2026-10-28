Feature: Operational dashboards and snapshots agree with the broker metric registry
  Scenario: Operational dashboards and alert rules reference real exported samples
    Given a fresh broker metric registry has one placeholder label set in every registered family
    And the checked in Grafana dashboard and alert rules define the operational series contract
    When the metric registry encodes its complete canonical exposition
    Then every referenced dashboard and alert series is actually exported and each document names at least ten series

  Scenario: The checked in exposition stays synchronized with the complete registry
    Given a fresh broker metric registry has one placeholder label set in every registered family
    And the checked in metrics body defines the complete encoded registry contract
    When the metric registry encodes its complete canonical exposition
    And the checked in metrics body is read after any requested regeneration
    Then the checked in body exactly matches the fresh canonical registry exposition
