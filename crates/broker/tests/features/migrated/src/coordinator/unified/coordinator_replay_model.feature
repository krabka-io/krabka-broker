Feature: Consumer group replay accepts valid log orderings and rejects orphaned deletion
  Scenario: Exhaustively explore bounded coordinator record orders
    Given a consumer group replay model includes child records tombstones and Kafka deletion order
    When breadth first search exhausts all bounded coordinator record orderings
    Then the search reaches exactly 6376 unique states without exhausting its bounds and all replay properties hold
