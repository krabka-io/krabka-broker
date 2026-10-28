Feature: Drained classic groups convert to streams while retaining committed offsets

  Scenario: drained classic group converts and preserves offsets
    Given an upgrade broker holds one-partition topic in for group g
    When a classic member joins and synchronizes the upgrade group
    And the classic upgrade member commits input offset forty-two
    Then the classic upgrade offset commit succeeds
    When the classic member leaves the upgrade group
    Then the classic upgrade member leaves successfully
    And the upgrade group is still Classic-typed
    When a streams member joins the drained classic group and converges
    Then the converted group accepts the streams heartbeat and becomes Streams-typed
    And OffsetFetch through the streams connection still returns input offset forty-two
