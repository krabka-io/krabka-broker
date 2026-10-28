Feature: Secured topic metadata supports publication and remote read-back

  Scenario: topic rlmm sasl loopback copy then fetch round trip
    Given the topic metadata manager and its client authenticate over SASL_PLAINTEXT as rlmm with password rlmm-secret
    When 80 records are appended to tiered-topic-rlmm-sasl-itest with 1-KiB segments and one byte local retention
    Then at least one completed segment is present in the remote archive
    When Fetch reads offset 0 through the topic-backed metadata manager
    Then the first fetched record value is test-record-0
