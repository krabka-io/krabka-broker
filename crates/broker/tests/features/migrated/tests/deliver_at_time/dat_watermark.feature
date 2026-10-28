Feature: tests deliver at time dat watermark

  Scenario: list offsets latest lands a seek to end on the first pending record
    Then deliver at time dat watermark: list offsets latest lands a seek to end on the first pending record

  Scenario: a batch scheduled far out holds back a later batch that is already due
    Then deliver at time dat watermark: a batch scheduled far out holds back a later batch that is already due
