Feature: Topic ACLs gate actual Produce and Fetch requests
  Scenario: produce denied without topic acl
    Given foo exists but alice has no topic grants
    When alice produces hello to foo partition 0 without a grant
    Then the produced partition reports error code 29

  Scenario: produce allowed with topic write acl
    Given alice has an Allow Write grant on literal topic foo
    When alice produces to foo after the Write grant applies
    Then the produced partition reports error code 0
    And foo partition 0 has at least one locally stored record

  Scenario: fetch denied without topic read acl
    Given foo exists but alice has no topic grants
    When alice fetches foo without a Read grant
    Then Fetch returns one topic and partition with topic authorization error 29
