@docker
Feature: Operators can interpret eligible leader replica reports consistently
  Scenario: Healthy partition ELR columns agree with Apache Kafka
    Given a single node broker and an Apache Kafka reference use the same ELR capable tool version
    When the operator creates and describes an identical two partition topic on both brokers
    Then successful partition reports match Apache Kafka including eligible leader replica columns
