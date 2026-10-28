Feature: tests metrics contract

  Scenario: every dashboard and rule series is one the registry exports
    Then metrics contract: every dashboard and rule series is one the registry exports

  Scenario: checked in body matches a fresh registry
    Then metrics contract: checked in body matches a fresh registry

  Scenario: sample names expand by metric type
    Then metrics contract: sample names expand by metric type

  Scenario: canonical body sorts counter samples and keeps bucket order
    Then metrics contract: canonical body sorts counter samples and keeps bucket order

  Scenario: referenced names are metric tokens only
    Then metrics contract: referenced names are metric tokens only
