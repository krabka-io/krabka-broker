Feature: Kafka follower Fetch refusal encoding
  A refusal carries no records or committed offsets. The leader endpoint is
  published only when that leader's identity is known, even if an endpoint alone
  remains in the local routing state.

  Scenario Outline: Refused Fetch bytes retain Kafka empty-response layout and routing
    Given a followed leader <known_leader> at epoch 5 with endpoint controller-2 port 9093
    When the follower encodes its refused Fetch response
    Then the wire bytes equal Kafka's empty Fetch response for leader <wire_leader> with endpoint present <endpoint>

    Examples:
      | known_leader | wire_leader | endpoint |
      | 2            | 2           | true     |
      | unknown      | -1          | false    |
