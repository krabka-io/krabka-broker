Feature: Heartbeat registration changes preserve Kafka ordering and registered epoch

  Scenario: fenced to unfenced
    Given broker 2 is registered at epoch 40 with fenced true and controlled shutdown false
    And the heartbeat moves from fenced to unfenced with partition changes none
    When the controller constructs heartbeat transition records
    Then the exact ordered heartbeat records are unfence

  Scenario: unfence before elections
    Given broker 2 is registered at epoch 40 with fenced true and controlled shutdown false
    And the heartbeat moves from fenced to unfenced with partition changes elect
    When the controller constructs heartbeat transition records
    Then the exact ordered heartbeat records are unfence,elect

  Scenario: fenced stays fenced
    Given broker 2 is registered at epoch 40 with fenced true and controlled shutdown false
    And the heartbeat moves from fenced to fenced with partition changes leave
    When the controller constructs heartbeat transition records
    Then the exact ordered heartbeat records are leave

  Scenario: fenced shuts down immediately
    Given broker 2 is registered at epoch 40 with fenced true and controlled shutdown false
    And the heartbeat moves from fenced to shutdown-now with partition changes leave
    When the controller constructs heartbeat transition records
    Then the exact ordered heartbeat records are leave

  Scenario: unfenced becomes fenced
    Given broker 2 is registered at epoch 40 with fenced false and controlled shutdown false
    And the heartbeat moves from unfenced to fenced with partition changes leave
    When the controller constructs heartbeat transition records
    Then the exact ordered heartbeat records are leave,fence

  Scenario: unfenced shuts down immediately
    Given broker 2 is registered at epoch 40 with fenced false and controlled shutdown false
    And the heartbeat moves from unfenced to shutdown-now with partition changes leave
    When the controller constructs heartbeat transition records
    Then the exact ordered heartbeat records are leave,fence

  Scenario: controlled shutdown begins before partition changes
    Given broker 2 is registered at epoch 40 with fenced false and controlled shutdown false
    And the heartbeat moves from unfenced to controlled-shutdown with partition changes leave
    When the controller constructs heartbeat transition records
    Then the exact ordered heartbeat records are controlled-shutdown,leave

  Scenario: controlled shutdown already recorded
    Given broker 2 is registered at epoch 40 with fenced false and controlled shutdown true
    And the heartbeat moves from unfenced to controlled-shutdown with partition changes leave
    When the controller constructs heartbeat transition records
    Then the exact ordered heartbeat records are leave

  Scenario: controlled shutdown finishes with fencing
    Given broker 2 is registered at epoch 40 with fenced false and controlled shutdown true
    And the heartbeat moves from controlled-shutdown to shutdown-now with partition changes leave
    When the controller constructs heartbeat transition records
    Then the exact ordered heartbeat records are leave,fence
