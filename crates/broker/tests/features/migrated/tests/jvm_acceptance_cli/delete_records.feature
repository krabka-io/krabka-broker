@docker
Feature: Operators trim retained records through Kafka tools
  Scenario: An operator trims a populated log through the stock command
    Given a single partition topic krabka-delete-recs-itest contains twenty acknowledged console records
    When the operator deletes records before offset 10
    Then the tool reports the low watermark at ten

  Scenario: A trim inside the active segment survives a restart
    Given a single partition topic krabka-delete-recs-restart-itest contains twenty acknowledged console records
    When the operator deletes records before offset 7
    Then the earliest readable offset is 7
    When the broker restarts on its existing log directory
    Then the earliest readable offset is 7
