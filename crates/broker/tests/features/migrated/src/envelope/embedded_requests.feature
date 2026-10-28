Feature: Kafka embedded request admission
  Header validity, forwardability, released API versions, and body decoding
  are checked in Kafka's order after the envelope's cluster authorization gate.

  Scenario: A truncated header is an unsupported version
    Given the embedded frame has only three header bytes
    When the controller unwraps the embedded request after cluster authorization succeeds
    Then the embedded request is rejected as UnsupportedVersion

  Scenario: Produce is refused as a non-forwardable API
    Given the embedded forwarded frame is:
      | api | version | body | unstable |
      | 0 | 9 | empty | false |
    When the controller unwraps the embedded request after cluster authorization succeeds
    Then the embedded request is rejected as InvalidRequest

  Scenario: A nested Envelope is refused before dispatch
    Given the embedded forwarded frame is:
      | api | version | body | unstable |
      | 58 | 0 | empty | false |
    When the controller unwraps the embedded request after cluster authorization succeeds
    Then the embedded request is rejected as InvalidRequest

  Scenario: Non-forwardability is checked before decoding a corrupt body
    Given the embedded forwarded frame is:
      | api | version | body | unstable |
      | 3 | 12 | garbage | false |
    When the controller unwraps the embedded request after cluster authorization succeeds
    Then the embedded request is rejected as InvalidRequest

  Scenario: The removed LeaderAndIsr API has no admitted version
    Given the embedded forwarded frame is:
      | api | version | body | unstable |
      | 4 | 0 | empty | false |
    When the controller unwraps the embedded request after cluster authorization succeeds
    Then the embedded request is rejected as UnsupportedVersion

  Scenario: Unstable mode does not restore removed ControlledShutdown
    Given the embedded forwarded frame is:
      | api | version | body | unstable |
      | 7 | 0 | empty | true |
    When the controller unwraps the embedded request after cluster authorization succeeds
    Then the embedded request is rejected as UnsupportedVersion

  Scenario: An API key above the released range is unknown
    Given the embedded forwarded frame is:
      | api | version | body | unstable |
      | 93 | 0 | empty | false |
    When the controller unwraps the embedded request after cluster authorization succeeds
    Then the embedded request is rejected as UnsupportedVersion

  Scenario: A negative API key is unknown even in unstable mode
    Given the embedded forwarded frame is:
      | api | version | body | unstable |
      | -1 | 0 | empty | true |
    When the controller unwraps the embedded request after cluster authorization succeeds
    Then the embedded request is rejected as UnsupportedVersion

  Scenario: Released mode refuses trunk-only UnregisterController
    Given the embedded forwarded frame is:
      | api | version | body | unstable |
      | 94 | 0 | UnregisterController-v0 | false |
    When the controller unwraps the embedded request after cluster authorization succeeds
    Then the embedded request is rejected as UnsupportedVersion

  Scenario: Unstable mode admits trunk-only UnregisterController
    Given the embedded forwarded frame is:
      | api | version | body | unstable |
      | 94 | 0 | UnregisterController-v0 | true |
    When the controller unwraps the embedded request after cluster authorization succeeds
    Then the embedded request is admitted for controller dispatch

  Scenario: A garbage CreateTopics body reports unsupported version
    Given the embedded forwarded frame is:
      | api | version | body | unstable |
      | 19 | 7 | garbage | false |
    When the controller unwraps the embedded request after cluster authorization succeeds
    Then the embedded request is rejected as UnsupportedVersion

  Scenario: A truncated CreateTopics body reports unsupported version
    Given the embedded forwarded frame is:
      | api | version | body | unstable |
      | 19 | 7 | truncated-CreateTopics-v7 | false |
    When the controller unwraps the embedded request after cluster authorization succeeds
    Then the embedded request is rejected as UnsupportedVersion

  Scenario: An unavailable request version is refused even with a valid body
    Given the embedded forwarded frame is:
      | api | version | body | unstable |
      | 19 | 99 | CreateTopics-v7 | false |
    When the controller unwraps the embedded request after cluster authorization succeeds
    Then the embedded request is rejected as UnsupportedVersion

  Scenario: A valid released CreateTopics request reaches controller dispatch
    Given the embedded forwarded frame is:
      | api | version | body | unstable |
      | 19 | 7 | CreateTopics-v7 | false |
    When the controller unwraps the embedded request after cluster authorization succeeds
    Then the embedded request is admitted for controller dispatch
