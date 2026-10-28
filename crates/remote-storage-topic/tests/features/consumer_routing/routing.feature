Feature: Metadata consumer routing and buffering
  Scenario: Event buffering respects the configured capacity
    Given the metadata event queue capacity is 2048
    When the metadata event channel is allocated
    Then the channel can buffer 2048 metadata events

  Scenario: Fetch routing follows the metadata topic leader
    Given remote metadata partition 2 is led by broker 9
    When leaders are requested for metadata partitions 2 and 1
    Then partition 2 routes to broker 9 and partition 1 has no leader
