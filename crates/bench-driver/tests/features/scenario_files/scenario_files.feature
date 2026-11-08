Feature: Operator benchmark scenario documents
  Scenario Outline: A shipped scenario describes a runnable benchmark
    Given the shipped benchmark scenario <filename> has been loaded
    When every loaded benchmark is serialized and reparsed
    Then every benchmark is runnable
    And every benchmark round trips through YAML

    Examples:
      | case                                    | filename                     |
      | a 512 KiB per second data rate          | datarate-512k.yaml           |
      | a long endurance run                    | endurance.yaml               |
      | a leader failover                       | failover.yaml                |
      | many partitions producers and consumers | fan-out.yaml                 |
      | a firehose of producers                 | firehose.yaml                |
      | latency at a fixed rate                 | fixed-rate-latency.yaml      |
      | fan-out across many partitions          | high-partition-fanout.yaml   |
      | latency across many partitions          | high-partition-latency.yaml  |
      | saturation across many partitions       | high-partition-saturate.yaml |
      | large messages                          | large-msg.yaml               |
      | acknowledgement by all replicas         | mixed-acks.yaml              |
      | saturation with small messages          | small-msg-saturate.yaml      |

  Scenario: Newly checked in scenarios join the validated corpus automatically
    Given all checked in benchmark scenarios have been loaded
    When every loaded benchmark is serialized and reparsed
    Then every benchmark is runnable
    And every benchmark round trips through YAML
