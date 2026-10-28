Feature: tests integration

  Scenario: list offsets by timestamp local
    Then integration: list offsets by timestamp local

  Scenario: end to end create produce fetch delete
    Then integration: end to end create produce fetch delete

  Scenario: produce acks zero sends no frame and keeps connection usable
    Then integration: produce acks zero sends no frame and keeps connection usable

  Scenario: second open recovers partitions from disk
    Then integration: second open recovers partitions from disk
