@docker
Feature: Ordinary users exchange records with literal topic and group grants
  Scenario: Literal topic and group grants permit the complete client record workflow
    Given ordinary user alice has literal Read Write on topic foo and Read on group cg foo
    When alice's non idempotent stock producer writes ten records to foo
    Then alice's authorized producer succeeds
    When alice's stock consumer joins cg foo and reads ten records
    Then the authorized group consumer emits every numbered record
