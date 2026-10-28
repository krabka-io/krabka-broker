Feature: UpdateFeatures atomically applies the last update for each named feature
  Scenario: Whole requests update features atomically and respect protocol result shape: version=1, updates=group.version:0:2;share.version:9:1, validate=false (1)
    Given a broker starts with finalized group.version 1
    And an UpdateFeatures version 1 request has updates group.version:0:2;share.version:9:1 and validate-only false
    When the broker applies the feature update request
    Then the feature response is atomically refused with successful feature rows none
    And group.version is 1 after the request

  Scenario: Whole requests update features atomically and respect protocol result shape: version=2, updates=group.version:0:2;share.version:9:1, validate=false (2)
    Given a broker starts with finalized group.version 1
    And an UpdateFeatures version 2 request has updates group.version:0:2;share.version:9:1 and validate-only false
    When the broker applies the feature update request
    Then the feature response is atomically refused with successful feature rows none
    And group.version is 1 after the request

  Scenario: Whole requests update features atomically and respect protocol result shape: version=0, updates=group.version:1:1, validate=false (3)
    Given a broker starts with finalized group.version 1
    And an UpdateFeatures version 0 request has updates group.version:1:1 and validate-only false
    When the broker applies the feature update request
    Then the feature response is accepted with successful feature rows group.version
    And group.version is 1 after the request

  Scenario: Whole requests update features atomically and respect protocol result shape: version=1, updates=group.version:0:2, validate=false (4)
    Given a broker starts with finalized group.version 1
    And an UpdateFeatures version 1 request has updates group.version:0:2 and validate-only false
    When the broker applies the feature update request
    Then the feature response is accepted with successful feature rows group.version
    And group.version is disabled after the request

  Scenario: Whole requests update features atomically and respect protocol result shape: version=2, updates=group.version:0:2, validate=false (5)
    Given a broker starts with finalized group.version 1
    And an UpdateFeatures version 2 request has updates group.version:0:2 and validate-only false
    When the broker applies the feature update request
    Then the feature response is accepted with successful feature rows none
    And group.version is disabled after the request

  Scenario: Whole requests update features atomically and respect protocol result shape: version=2, updates=group.version:0:2, validate=true (6)
    Given a broker starts with finalized group.version 1
    And an UpdateFeatures version 2 request has updates group.version:0:2 and validate-only true
    When the broker applies the feature update request
    Then the feature response is accepted with successful feature rows none
    And group.version is 1 after the request

  Scenario: Whole requests update features atomically and respect protocol result shape: version=1, updates=none, validate=false (7)
    Given a broker starts with finalized group.version 1
    And an UpdateFeatures version 1 request has updates none and validate-only false
    When the broker applies the feature update request
    Then the feature response is accepted with successful feature rows none
    And group.version is 1 after the request

  Scenario: Whole requests update features atomically and respect protocol result shape: version=1, updates=group.version:9:1;group.version:0:2, validate=false (8)
    Given a broker starts with finalized group.version 1
    And an UpdateFeatures version 1 request has updates group.version:9:1;group.version:0:2 and validate-only false
    When the broker applies the feature update request
    Then the feature response is accepted with successful feature rows group.version,group.version
    And group.version is disabled after the request

