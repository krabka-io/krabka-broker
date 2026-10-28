Feature: tests kip559 l7 proxy fields

  Scenario: join group response carries protocol type and name on success
    Then kip559 l7 proxy fields: join group response carries protocol type and name on success

  Scenario: sync group response carries protocol type and name on success
    Then kip559 l7 proxy fields: sync group response carries protocol type and name on success

  Scenario: join group inconsistent protocol error carries no protocol fields
    Then kip559 l7 proxy fields: join group inconsistent protocol error carries no protocol fields

  Scenario: sync group error carries no protocol fields
    Then kip559 l7 proxy fields: sync group error carries no protocol fields
