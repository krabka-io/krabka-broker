Feature: Kafka request forwarding policy
  The declared forwarding surface agrees with Kafka's ApiKeys.forwardable.
  Forwarding never permits recursive envelopes or data-plane Produce requests.

  Scenario: Declared request forwarding agrees with admission and Kafka's public set
    Given the router uses its declared Envelope forwarding policy
    When the forwardability predicate checks the declared requests, Envelope, and Produce
    Then the declared and admitted forwardable API keys are exactly:
      | key |
      | 19 |
      | 20 |
      | 30 |
      | 31 |
      | 33 |
      | 37 |
      | 38 |
      | 39 |
      | 40 |
      | 43 |
      | 44 |
      | 45 |
      | 46 |
      | 49 |
      | 51 |
      | 55 |
      | 57 |
      | 64 |
      | 67 |
      | 80 |
      | 81 |
      | 94 |
    And neither a nested Envelope nor a data-plane Produce request is forwardable
