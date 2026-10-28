Feature: Authorization denial keeps the audit broker responsive
  Scenario: denied operation returns topic authorization failed
    Given a broker whose authorizer denies every operation
    When an anonymous client creates denied-topic
    Then CreateTopics returns topic authorization error 29
    When the same client fetches the audit topic after the denial
    Then the broker still returns a decoded Fetch response
