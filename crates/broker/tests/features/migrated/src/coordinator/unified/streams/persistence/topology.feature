Feature: src coordinator unified streams persistence topology

  @topology
  Scenario: Persist topology epoch 1 has subtopology 0 reading s and changelog c with replication factor 3
    Given topology epoch 1 has subtopology 0 reading s and changelog c with replication factor 3
    When the coordinator persists the Streams topology in the compacted group log
    Then the persisted Streams topology matches Kafka schema bytes including compact lengths and tagged trailers
