Feature: src kafka log consumer

  Scenario: tests: initial connection retries without reassignment
    Then kafka log consumer tests: initial connection retries without reassignment

  Scenario: tests: initial connection and retry delay are cancellable
    Then kafka log consumer tests: initial connection and retry delay are cancellable

  Scenario: tests: metadata event channel uses configured capacity
    Then kafka log consumer tests: metadata event channel uses configured capacity

  Scenario: tests: assignment handle tracks spawned partitions
    Then kafka log consumer tests: assignment handle tracks spawned partitions

  Scenario: tests: partition fetches route to the metadata leader
    Then kafka log consumer tests: partition fetches route to the metadata leader
