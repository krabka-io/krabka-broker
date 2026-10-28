Feature: tests metrics

  Scenario: rejected client labels leave the metrics body
    Then metrics: rejected client labels leave the metrics body

  Scenario: metrics endpoint serves openmetrics and counters tick
    Then metrics: metrics endpoint serves openmetrics and counters tick

  Scenario: partition level metrics and disk gauge render
    Then metrics: partition level metrics and disk gauge render
