Feature: Kafka-compatible key partitioning
  Brokers and Kafka clients must agree on the hash and destination partition
  for the same key, including keys shorter than a four-byte hash block.

  Scenario Outline: Murmur2 preserves Kafka's reference vectors at every remainder length
    Given the Kafka hash key is <key>
    When the key is hashed with Kafka Murmur2
    Then the unsigned hash is <hash>

    Examples:
      | key     | hash       |
      | (empty) | 275646681  |
      | a       | 2731586172 |
      | ab      | 316155434  |
      | abc     | 479470107  |
      | abcd    | 2971317748 |
      | abcde   | 461995741  |

  Scenario Outline: Transaction keys select the same partition as Kafka clients
    Given the Kafka hash key is <key>
    And the destination topic has <partitions> partitions
    When Kafka's absolute-hash partition rule is applied
    Then the destination partition is <partition>

    Examples:
      | key            | partitions | partition |
      | my-tid         | 50         | 43        |
      | producer-1     | 50         | 45        |
      | tx-orders-prod | 50         | 26        |
      | abcde          | 7          | 4         |
