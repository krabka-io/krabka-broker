Feature: Topic metadata activation and remote reads use the loopback listener

  Background:
    Given a topic-backed metadata manager bootstraps against its broker's plaintext loopback listener
    When the topic-backed manager finishes activation

  Scenario: topic rlmm activates against loopback
    Then the broker hosts __remote_log_metadata partition 0

  Scenario: topic rlmm copy then fetch round trip
    When 80 records are appended to tiered-topic-rlmm-itest with 1-KiB segments and one byte local retention
    Then at least one completed segment is present in the remote archive
    When Fetch reads offset 0 through the topic-backed metadata manager
    Then the first fetched record value is test-record-0
