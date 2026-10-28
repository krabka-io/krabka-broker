@docker
Feature: Legacy compressed message wrappers convert into modern record batches
  Scenario: Gzip wrapped legacy records are readable by a modern consumer
    Given a legacy compatible broker hosts compression topic legacy-010-compressed-round-trip
    When the legacy producer writes fifty numbered records compressed with gzip
    Then the compressed legacy producer succeeds
    When the modern console consumer reads fifty records from the compression topic
    Then all fifty numbered records survive legacy decompression and format conversion

  Scenario: Snappy wrapped legacy records are readable by a modern consumer
    Given a legacy compatible broker hosts compression topic legacy-010-snappy-round-trip
    When the legacy producer writes fifty numbered records compressed with snappy
    Then the compressed legacy producer succeeds
    When the modern console consumer reads fifty records from the compression topic
    Then all fifty numbered records survive legacy decompression and format conversion
