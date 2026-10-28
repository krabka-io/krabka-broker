Feature: tests client quotas admin api

  Scenario: alter then describe round trip
    Then client quotas admin api: alter then describe round trip

  Scenario: non super user denied
    Then client quotas admin api: non super user denied
