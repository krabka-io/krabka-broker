@docker
Feature: A quorum serves stock clients across brokers and survives controller loss
  Scenario: Stock clients exchange records across brokers and metadata survives controller loss
    Given a three node controller quorum has created its topic and propagated it to the second broker
    When the stock producer bootstraps through broker two and writes a b and c
    Then the cross broker stock producer succeeds
    When the stock consumer bootstraps through broker three and reads the topic
    Then the consumer receives a b and c
    When the controller leader shuts down and an operator lists topics through a surviving broker
    Then the surviving quorum still reports the original topic
