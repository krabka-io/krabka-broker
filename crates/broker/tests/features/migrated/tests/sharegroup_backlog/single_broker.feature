Feature: Share backlog persists without consumers and follows the durable start offset

  Scenario: backlog is scraped and survives scale to zero
    Given a one-partition RF1 topic contains five records and share backlog is sampled every 50 milliseconds
    When member-1 joins backlog-workers and initializes its share-state cursor
    Then the registry and Prometheus scrape report partition 0 backlog 5
    When the final group member leaves
    Then the durable share-state cursor and scraped backlog 5 survive scale to zero
    When AlterShareGroupOffsets advances partition 0 start offset to 5
    Then the registry and Prometheus scrape report partition 0 backlog 0
    When the topic is deleted
    Then the deleted topic and its backlog metric series disappear
