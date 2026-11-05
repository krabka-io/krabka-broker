Feature: Fatal metadata controller startup faults terminate the broker

  Scenario: Unsupported finalized metadata levels halt startup
    Given standalone node 1 is formatted at unstable metadata release 4.5-IV0
    When the broker binary starts node 1 without unstable feature versions enabled
    Then the broker exits within 60 seconds with status 1 and reports a fatal controller halt
    And stderr contains the Kafka fatal fault "Encountered fatal fault: Tried to apply FeatureLevelRecord FeatureLevelRecord(name='metadata.version', featureLevel=34), but this controller only supports versions 7-33"

  Scenario: Controller activation refuses duplicate bootstrap topic names
    Given standalone node 1 is formatted with these bootstrap topic records
      | name | UUID | partitions | replication factor |
      | orders | 1 | 1 | 1 |
      | orders | 2 | 1 | 1 |
    When the broker binary starts node 1 without unstable feature versions enabled
    Then the broker exits within 60 seconds with status 1 and reports a fatal controller halt
    And stderr contains the Kafka fatal fault "Encountered fatal fault: exception while completing controller activation: metadata: topic 'orders' already exists"
