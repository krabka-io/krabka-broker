Feature: Released API version negotiation
  A client negotiating the default broker protocol sees the released Kafka
  ranges, the intentional legacy Produce range, and no invented public API keys.

  Scenario: Default API negotiation follows the released Kafka protocol
    Given Kafka 4.3.1's released API version ranges
    When an inter-broker client requests API versions with metrics configured and both compatibility opt-ins disabled
    Then the advertised ranges match Kafka's release except Produce starts at version zero and private APIs remain private
