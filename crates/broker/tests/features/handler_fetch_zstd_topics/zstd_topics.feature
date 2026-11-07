Feature: Consumers older than Fetch v10 cannot read zstd topics

  KIP-110 added zstd with Fetch v10, so a consumer that sends an older Fetch
  cannot be assumed to decompress zstd. Kafka's KafkaApis.handleFetchRequest
  refuses such a consumer every partition of a topic whose compression.type is
  zstd, whatever codec the stored batches use. Follower fetches are not
  refused. The per-batch refusal of zstd on the legacy Fetch v0 to v3 path is a
  separate rule.

  Rule: A consumer older than Fetch v10 is refused a topic configured for zstd

    Scenario: A Fetch v9 consumer is refused a zstd topic
      Given a topic with compression.type zstd holds one record
      When a consumer fetches the topic with Fetch v9
      Then the partition answers UNSUPPORTED_COMPRESSION_TYPE with no records

    Scenario: A Fetch v9 consumer reads a gzip topic
      Given a topic with compression.type gzip holds one record
      When a consumer fetches the topic with Fetch v9
      Then the consumer receives the record

  Rule: Fetch v10 is the first version that reads a zstd topic

    Scenario: A Fetch v10 consumer reads a zstd topic
      Given a topic with compression.type zstd holds one record
      When a consumer fetches the topic with Fetch v10
      Then the consumer receives the record
