Feature: Dead-letter topics opt into Kafka dead-letter delivery with creation defaults

  Scenario: Build a dead-letter topic that uses cluster topology defaults
    Given the requested automatic topic name is dlq.orders
    When the manager builds the dead-letter topic definition
    Then the complete automatic topic definition is:
      | field | value |
      | name | dlq.orders |
      | partitions | -1 |
      | replication | -1 |
      | configs | [["errors.deadletterqueue.group.enable","true"],["message.timestamp.type","CreateTime"]] |
