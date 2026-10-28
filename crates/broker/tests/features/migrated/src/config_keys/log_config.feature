Feature: Topic overrides configure the partition log
  The partition log must honor Kafka topic settings while retaining all other defaults.

  Scenario: Apply rolling, indexing, compaction and timestamp policy together
    Given a partition has these topic log overrides:
      | key | value |
      | segment.ms | 60000 |
      | index.interval.bytes | 8192 |
      | segment.index.bytes | 1048576 |
      | min.compaction.lag.ms | 30000 |
      | max.compaction.lag.ms | 120000 |
      | min.cleanable.dirty.ratio | 0.25 |
      | message.timestamp.type | LogAppendTime |
    When the overrides are applied to the default partition log configuration
    Then rolling, index sizing, compaction and append timestamps use the supplied values
