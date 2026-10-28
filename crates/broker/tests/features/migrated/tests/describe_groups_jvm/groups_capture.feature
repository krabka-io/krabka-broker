Feature: Real Kafka defines classic DescribeGroups projections

  Scenario: capture real kafka describe groups
    Given cp-kafka 7.4.0 hosts classic range group g and offset-only group simple-typeless
    When DescribeGroups requests groups g,simple-typeless without authorized operations
    And the classic and typeless responses are captured as real_kafka_classic.json
    Then classic group g has error 0, protocol consumer, assignor range, state Stable, and one version-3 metadata member
    And offset-only group simple-typeless has error 0 and empty protocol and assignor
    Given Apache Kafka 4.0.0 hosts consumer-protocol group g-next
    When DescribeGroups requests groups g-next without authorized operations
    And the next-generation response is captured as real_kafka_next_gen.json
    Then consumer group g-next has error 69, state Dead, no classic protocol or members, and message Group g-next is not a classic group.
