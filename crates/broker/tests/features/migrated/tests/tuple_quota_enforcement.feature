Feature: Producer byte quotas match both user and client ID

  Scenario: tuple quota throttles only matching client id
    Given Alice may write tuple-quota-topic using authenticated client IDs
    When the administrator assigns only Alice's app-x tuple a 1024-byte-per-second producer quota
    Then the tuple quota alteration succeeds and appears in committed metadata
    When Alice produces four kilobytes through a fresh connection with client ID app-x
    Then the tuple-quota produce succeeds and is throttled: true
    When Alice produces four kilobytes through a fresh connection with client ID other
    Then the tuple-quota produce succeeds and is throttled: false
