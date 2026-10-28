Feature: Kafka compression-level topic bounds
  Each codec accepts the same minimum and maximum settings as Kafka's validator.

  Scenario Outline: Compression level boundary values remain accepted
    Given an operator alters topic key <key> to quoted value <value>
    When the released topic alteration validator checks the operator value
    Then the individual topic alteration is accepted

    Examples:
      | key | value |
      | compression.gzip.level | "-1" |
      | compression.gzip.level | "1" |
      | compression.gzip.level | "9" |
      | compression.lz4.level | "1" |
      | compression.lz4.level | "17" |
      | compression.zstd.level | "-131072" |
      | compression.zstd.level | "22" |
