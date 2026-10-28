Feature: Soak resource judgments distinguish leaks from steady churn
  Background:
    Given resource bounds allow ceiling 1000 drift fraction 0.20 and floor 10

  Scenario: tests: a flat series is level
    Given 40 resource samples are constant at 500
    When the soak judge evaluates the resource series
    Then the flat resource verdict is Level with absolute fitted rise below one

  Scenario: tests: noise around a flat mean is level
    Given 40 resource samples repeat values 500,512,488,505,495,517,483,501
    When the soak judge evaluates the resource series
    Then the resource series does not fail its bound

  Scenario: tests: a rising series drifts
    Given 40 resource samples start at 300 and change by 8
    When the soak judge evaluates the resource series
    Then the rising verdict describes drift from seconds 200 through 390 exceeding 140 widgets

  Scenario: tests: a segment roll sawtooth is level
    Given ten retention sawteeth have six samples with base 400 amplitude 120 and baseline rise 0
    When the soak judge evaluates the resource series
    Then the resource series does not fail its bound

  Scenario: tests: a sawtooth on a rising baseline drifts
    Given ten retention sawteeth have six samples with base 400 amplitude 120 and baseline rise 40
    When the soak judge evaluates the resource series
    Then the rising retention baseline fails with a Drift verdict

  Scenario: tests: a ceiling breach names the sample
    Given 40 resource samples are constant at 500
    And resource sample 3 is replaced by 1400
    When the soak judge evaluates the resource series
    Then the ceiling verdict identifies 1400 widgets at second 30 and names the 1000 widget ceiling

  Scenario: tests: the ceiling outranks a flat trend
    Given 40 resource samples are constant at 500
    And resource sample 39 is replaced by 1001
    When the soak judge evaluates the resource series
    Then the resource series fails its bound

  Scenario: tests: too few samples is not a pass
    Given 4 resource samples are constant at 500
    When the soak judge evaluates the resource series
    Then the verdict rejects too few second-half observations

  Scenario: An empty series cannot pass
    Given 0 resource samples are constant at 500
    When the soak judge evaluates the resource series
    Then the resource series fails its bound

  Scenario: tests: a falling series is level
    Given 40 resource samples start at 900 and change by -5
    When the soak judge evaluates the resource series
    Then the resource series does not fail its bound

  Scenario: tests: the floor protects a near zero series
    Given 40 resource samples repeat values 0,1,2,3,4
    When the soak judge evaluates the resource series
    Then the resource series does not fail its bound

  Scenario: tests: judge all reports one verdict per series
    Given named rising and flat resource series occur in that order
    When the soak judge evaluates all named series together
    Then rising fails and flat passes with named verdicts in input order
