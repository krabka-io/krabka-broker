Feature: Independent Sarama wire behavior agrees with native broker APIs

  Scenario: sarama round trip and cluster views agree with krabka
    Given sarama-conformance has three RF1 partitions on the host broker
    Then kaf topics lists sarama-conformance with three partitions and one replica
    When kaf produce sends hello-from-sarama to partition 0
    Then the produce receipt reports partition 0 offset 0
    When kaf consume joins sarama-conformance-group, reads the payload and commits its offset before the container stops
    Then the member identifies as sarama, stdout is exactly the payload and only partition 0 is committed at offset 1
    When the native client reads DescribeCluster and all DescribeTopicPartitions rows
    Then kaf nodes and topic describe match the native cluster and all three partition rows
