Feature: Share fetch session epochs validate clients and retain cached partitions until forgotten

  Background:
    Given a share member has one available record and no open fetch session

  Scenario: session epoch validation
    When the share member fetches using session epoch 0
    Then the share session response reports NONE
    When the share member fetches using session epoch 9
    Then the share session response reports INVALID_SHARE_SESSION_EPOCH
    When another joined member without a fetch session requests epoch five
    Then the share session response reports SHARE_SESSION_NOT_FOUND

  Scenario: incremental session uses cached and forgotten partitions
    When the session member opens a fetch and acquires the available record
    Then the opening session acquires one record
    When the member accepts the first session record and another record is produced
    And the member sends an empty incremental fetch at epoch 2
    Then the cached session returns partition zero with one acquired record
    When the member accepts cached offset one at epoch three
    And the member forgets partition zero at session epoch four
    Then the session succeeds with no topic responses
    When one further record is produced for the forgotten session partition
    And the member sends an empty incremental fetch at epoch 5
    Then the session succeeds with no topic responses
