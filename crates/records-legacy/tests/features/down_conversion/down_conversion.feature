Feature: v2 record batches down-convert to legacy message sets

  A consumer on Fetch v0 or v1 reads magic 0 messages, and one on Fetch v2
  or v3 reads magic 1 messages. The broker down-converts each stored v2
  record batch for them, as Apache Kafka's RecordsUtil.downConvert does.
  RecordsUtil.downConvert skips a control batch and then refuses a zstd
  batch, because a legacy message cannot name zstd, and the broker answers
  that partition with UNSUPPORTED_COMPRESSION_TYPE.
  The legacy layouts are in message_format/message_format.feature and
  compressed_wrappers/compressed_wrappers.feature.

  Rule: Control batches are not sent to a legacy consumer

    Scenario: A transaction marker down-converts to nothing
      Given a v2 control batch with a commit marker
      When the broker down-converts the batch to magic 1
      Then the legacy message set is empty

  Rule: Offsets, keys and values carry over and headers are dropped

    Scenario: A record with headers down-converts without them
      Given a v2 batch at base offset 1000 whose record at offset delta 2 has a key, a value and headers
      When the broker down-converts the batch to magic 1
      Then the legacy record at offset 1002 has the same key and value

  Rule: Magic 0 drops the timestamp and magic 1 keeps the absolute timestamp

    Scenario Outline: A record timestamp is the batch base timestamp plus the record delta
      Given a v2 batch with base timestamp 1700000000000 whose record has timestamp delta 100
      When the broker down-converts the batch to magic <magic>
      Then the legacy record has <timestamp>

      Examples:
        | case                 | magic | timestamp               |
        | magic 0 has no field | 0     | no timestamp            |
        | magic 1 is absolute  | 1     | timestamp 1700000000100 |

  Rule: A codec that legacy messages can name carries over to a wrapper

    Scenario Outline: A compressed v2 batch down-converts to a wrapper with the same codec
      Given a v2 batch compressed with <codec>
      When the broker down-converts the batch to magic 1
      Then the legacy message set is one wrapper compressed with <codec>

      Examples:
        | case   | codec  |
        | code 1 | gzip   |
        | code 2 | snappy |
        | code 3 | lz4    |

    Scenario: An uncompressed v2 batch down-converts to one message per record
      Given a v2 batch of three uncompressed records
      When the broker down-converts the batch to magic 1
      Then the legacy message set is three uncompressed messages

  Rule: A zstd batch is not down-converted

    Scenario: A zstd batch is refused as an unsupported compression type
      Given a v2 batch compressed with zstd
      When the broker down-converts the batch to magic 1
      Then the down-conversion is refused with UNSUPPORTED_COMPRESSION_TYPE

  Rule: A LogAppendTime batch down-converts with its log append time

    @inprogress
    Scenario: Every message of a LogAppendTime batch carries the batch max timestamp
      Given a v2 LogAppendTime batch with max timestamp 1700000005000
      When the broker down-converts the batch to magic 1
      Then every legacy message is LogAppendTime with timestamp 1700000005000
