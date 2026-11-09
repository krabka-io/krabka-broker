Feature: Remote log metadata is partitioned by topic ID and partition

  Rule: A partition's metadata lands in the metadata partition Kafka's partitioner picks

    Scenario Outline: The metadata partition matches the JVM partitioner
      Given a topic with topic ID 12345678-9abc-def0-0011-223344556677
      When a broker places the metadata of partition <partition> in a metadata topic of 50 partitions
      Then the metadata lands in metadata partition <expected>

      Examples:
        | case                                            | partition | expected |
        | partition zero is hashed, not mapped to zero    | 0         | 6        |
        | the next partition lands far from its neighbour | 1         | 36       |

  Rule: A topic's name plays no part in where its metadata lands

    Scenario: A renamed topic keeps its metadata partition
      Given the metadata of orders partition 3 has been placed in a metadata topic of 50 partitions
      When a broker places the metadata of partition 3 of the topic renamed to invoices
      Then the metadata lands in the same metadata partition as before

  Rule: A metadata topic with one partition carries the metadata of every partition

    Scenario: Every partition's metadata lands in the only metadata partition
      When a broker places the metadata of partitions 0 to 19 in a metadata topic of 1 partition
      Then every partition's metadata lands in metadata partition 0

  Rule: A metadata topic needs at least one partition

    Scenario: A metadata topic with no partitions is refused
      When a broker attempts to place the metadata of partition 0 in a metadata topic of 0 partitions
      Then the placement is refused because the metadata topic has 0 partitions
