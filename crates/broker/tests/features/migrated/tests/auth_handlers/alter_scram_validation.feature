Feature: tests auth handlers alter scram validation

  Scenario: alter scram creds low iterations rejected
    Then auth handlers alter scram validation: alter scram creds low iterations rejected

  Scenario: alter scram creds high iterations rejected but max allowed
    Then auth handlers alter scram validation: alter scram creds high iterations rejected but max allowed

  Scenario: alter scram creds unknown mechanism returns unsupported sasl mechanism
    Then auth handlers alter scram validation: alter scram creds unknown mechanism returns unsupported sasl mechanism

  Scenario: alter scram creds duplicate resource rejected
    Then auth handlers alter scram validation: alter scram creds duplicate resource rejected

  Scenario: alter scram creds duplicate deletion and upsertion rejected per user
    Then auth handlers alter scram validation: alter scram creds duplicate deletion and upsertion rejected per user

  Scenario: alter scram creds missing deletion returns resource not found 91
    Then auth handlers alter scram validation: alter scram creds missing deletion returns resource not found 91
