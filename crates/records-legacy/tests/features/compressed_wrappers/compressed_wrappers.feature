Feature: Compressed legacy messages wrap one nested message set

  A compressed legacy message set is a single wrapper message. The low
  three bits of its attributes name the codec, and its value is the
  compressed bytes of an inner message set that holds the records. These
  rules follow Apache Kafka's AbstractLegacyRecordBatch and
  MemoryRecordsBuilder. The uncompressed layout is in
  message_format/message_format.feature.

  Rule: The low three bits of the attributes name the codec

    Scenario Outline: A wrapper is decompressed with the codec its attributes name
      Given a magic 1 wrapper compressed with <codec>
      When the broker decodes the message set
      Then the broker reads every inner record

      Examples:
        | case   | codec  |
        | code 1 | gzip   |
        | code 2 | snappy |
        | code 3 | lz4    |

    Scenario Outline: A compression code that legacy messages cannot carry is rejected
      Given a magic 1 message with compression code <code>
      When the broker decodes the message set
      Then the message set is rejected as invalid

      Examples:
        | case                            | code |
        | zstd exists only in magic 2     | 4    |
        | highest code the three bits fit | 7    |

  Rule: A magic 0 wrapper holds absolute inner offsets and carries the last one

    Scenario: A magic 0 wrapper carries the offset of its last record
      Given records at offsets 100, 101 and 102
      When a legacy producer compresses them with gzip under magic 0
      Then the wrapper offset is 102
      And the inner offsets are 100, 101 and 102

  Rule: A magic 1 wrapper holds relative inner offsets that end at the wrapper offset

    Scenario: A magic 1 wrapper writes inner offsets relative to its first record
      Given records at offsets 100, 102 and 105
      When a legacy producer compresses them with gzip under magic 1
      Then the wrapper offset is 105
      And the inner offsets are 0, 2 and 5

    Scenario Outline: A magic 1 wrapper anchors its last inner offset at the wrapper offset
      Given a magic 1 wrapper at offset <wrapper> around inner offsets <inner>
      When the broker decodes the message set
      Then the broker reads records at offsets <absolute>

      Examples:
        | case                                 | wrapper | inner   | absolute      |
        | contiguous records                   | 102     | 0, 1, 2 | 100, 101, 102 |
        | records removed by compaction        | 105     | 0, 2, 5 | 100, 102, 105 |
        | wrapper offset 0 from old librdkafka | 0       | 0, 1, 2 | 0, 1, 2       |

    Scenario: A magic 1 wrapper offset below its last inner offset is rejected
      Given a magic 1 wrapper at offset 1 around inner offsets 0, 1, 2
      When the broker decodes the message set
      Then the message set is rejected as invalid

  Rule: A wrapper holds one level of compression

    Scenario: A compressed wrapper inside a compressed wrapper is rejected
      Given a gzip wrapper whose inner message set is itself a gzip wrapper
      When the broker decodes the message set
      Then the message set is rejected for nested compression

  Rule: Inner messages carry the magic of their wrapper

    Scenario: A magic 1 wrapper around magic 0 messages is rejected
      Given a magic 1 gzip wrapper around magic 0 messages
      When the broker decodes the message set
      Then the message set is rejected for inconsistent magic

  Rule: The timestamp type of a magic 1 wrapper decides the inner timestamps

    Scenario: A CreateTime wrapper keeps the timestamp of each inner record
      Given a magic 1 CreateTime wrapper with timestamp 3000 around records with timestamps 1000 and 2000
      When the broker decodes the message set
      Then the broker reads records with timestamps 1000 and 2000

    Scenario: A LogAppendTime wrapper gives every inner record the wrapper timestamp
      Given a magic 1 LogAppendTime wrapper with timestamp 3000 around records with timestamps 1000 and 2000
      When the broker decodes the message set
      Then the broker reads records with timestamps 3000 and 3000

    Scenario: A magic 1 wrapper carries the latest inner timestamp
      Given records with timestamps 1000, 3000 and 2000
      When a legacy producer compresses them with gzip under magic 1
      Then the wrapper timestamp is 3000
