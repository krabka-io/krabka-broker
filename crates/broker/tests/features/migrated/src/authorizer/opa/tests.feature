Feature: Authorization policies receive the published Kafka plugin envelope
  Existing OPA policies match action and requestContext fields when authorizing Kafka requests.

  Scenario: Published Kafka plugin policy receives action and context
    Given an OPA policy permits only this published Kafka plugin request:
      """
      {"input": {
        "action": {
          "operation": "READ",
          "resourcePattern": {"resourceType": "TOPIC", "name": "orders", "patternType": "LITERAL", "unknown": false},
          "resourceReferenceCount": 1, "logIfAllowed": true, "logIfDenied": true
        },
        "requestContext": {
          "principal": {"principalType": "User", "name": "alice"},
          "clientAddress": "/1.2.3.4"
        }
      }}
      """
    When alice at 1.2.3.4 requests permission to read topic orders
    Then OPA receives the action and principal context once and allows the read
