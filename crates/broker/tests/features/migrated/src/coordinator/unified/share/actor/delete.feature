Feature: DeleteGroups removes Share state before empty group tombstones
  Scenario: A missing Share group is not found
    Given the Share coordinator has a persister true and no brokers in its metadata image
    And Share group sg at epoch 3 has orders topic UUID 09090909-0909-0909-0909-090909090909 and this persisted state
      | exists | members | initialized | initializing | deleting |
      | false | 0 | [] | [] | false |
    When DeleteGroups deletes sg and the coordinator observes its registry and appended records
    Then Share deletion returns not-found and the group remains registered false
    And the complete Share deletion log contains these records in order
      | family | value |
  Scenario: A Share group with members cannot be deleted
    Given the Share coordinator has a persister true and no brokers in its metadata image
    And Share group sg at epoch 3 has orders topic UUID 09090909-0909-0909-0909-090909090909 and this persisted state
      | exists | members | initialized | initializing | deleting |
      | true | 1 | [] | [] | false |
    When DeleteGroups deletes sg and the coordinator observes its registry and appended records
    Then Share deletion returns non-empty and the group remains registered true
    And the complete Share deletion log contains these records in order
      | family | value |
  Scenario: An empty Share group without state deletes without a persister
    Given the Share coordinator has a persister false and no brokers in its metadata image
    And Share group sg at epoch 3 has orders topic UUID 09090909-0909-0909-0909-090909090909 and this persisted state
      | exists | members | initialized | initializing | deleting |
      | true | 0 | [] | [] | false |
    When DeleteGroups deletes sg and the coordinator observes its registry and appended records
    Then Share deletion returns deleted and the group remains registered false
    And the complete Share deletion log contains these records in order
      | family | value |
      | target-epoch | tombstone |
      | state-metadata | tombstone |
      | group-epoch | tombstone |
  Scenario: Initialized Share state is marked deleting before an unavailable coordinator refuses deletion
    Given the Share coordinator has a persister true and no brokers in its metadata image
    And Share group sg at epoch 3 has orders topic UUID 09090909-0909-0909-0909-090909090909 and this persisted state
      | exists | members | initialized | initializing | deleting |
      | true | 0 | [0,1] | [] | false |
    When DeleteGroups deletes sg and the coordinator observes its registry and appended records
    Then Share deletion returns coordinator-unavailable and the group remains registered true
    And the complete Share deletion log contains these records in order
      | family | value |
      | state-metadata | {"initializing":[],"initialized":[],"deleting":[{"topic_id":"09090909-0909-0909-0909-090909090909","topic_name":"orders"}]} |
  Scenario: Deleting state whose topic disappeared is cleared before group tombstones
    Given the Share coordinator has a persister true and no brokers in its metadata image
    And Share group sg at epoch 3 has orders topic UUID 09090909-0909-0909-0909-090909090909 and this persisted state
      | exists | members | initialized | initializing | deleting |
      | true | 0 | [] | [] | true |
    When DeleteGroups deletes sg and the coordinator observes its registry and appended records
    Then Share deletion returns deleted and the group remains registered false
    And the complete Share deletion log contains these records in order
      | family | value |
      | state-metadata | {"initializing":[],"initialized":[],"deleting":[]} |
      | target-epoch | tombstone |
      | state-metadata | tombstone |
      | group-epoch | tombstone |
  Scenario: Initialized Share state requires a wired persister
    Given the Share coordinator has a persister false and no brokers in its metadata image
    And Share group sg at epoch 3 has orders topic UUID 09090909-0909-0909-0909-090909090909 and this persisted state
      | exists | members | initialized | initializing | deleting |
      | true | 0 | [0] | [] | false |
    When DeleteGroups deletes sg and the coordinator observes its registry and appended records
    Then Share deletion returns coordinator-unavailable and the group remains registered true
    And the complete Share deletion log contains these records in order
      | family | value |
  Scenario: Initializing Share state also requires a wired persister
    Given the Share coordinator has a persister false and no brokers in its metadata image
    And Share group sg at epoch 3 has orders topic UUID 09090909-0909-0909-0909-090909090909 and this persisted state
      | exists | members | initialized | initializing | deleting |
      | true | 0 | [] | [0] | false |
    When DeleteGroups deletes sg and the coordinator observes its registry and appended records
    Then Share deletion returns coordinator-unavailable and the group remains registered true
    And the complete Share deletion log contains these records in order
      | family | value |
