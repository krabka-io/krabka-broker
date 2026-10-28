Feature: tests auth handlers alter scram

  Scenario: alter scram creds super user can provision
    Then auth handlers alter scram: alter scram creds super user can provision

  Scenario: alter scram creds super user can provision sha256
    Then auth handlers alter scram: alter scram creds super user can provision sha256

  Scenario: alter scram creds non super user rejected
    Then auth handlers alter scram: alter scram creds non super user rejected
