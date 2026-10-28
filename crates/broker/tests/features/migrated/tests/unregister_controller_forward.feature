Feature: Followers forward controller administration and relay its exact answers

  Scenario: a follower forwards unregister controller to the active controller
    Given two trunk-version controller voters hold controller 7's registration
    When the follower receives the metadata-version upgrade required by UnregisterController
    Then the forwarded metadata-version upgrade succeeds
    When the follower receives two UnregisterController requests for controller 7
    Then the follower relays the successful removal and repeated missing-registration answers unchanged
    And the active controller image no longer contains controller 7
