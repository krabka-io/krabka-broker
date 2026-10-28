Feature: Streams topic completion caches Kafka errors until the exact expiry boundary

  Background:
    Given streams topics a, b, c and d are being created

  Scenario: A controller response caches per-topic default and explicit messages
    Given the controller answered topic creation with:
      | name | code | message |
      | a | 0 | null |
      | b | 36 | "Topic 'b' already exists." |
      | c | 40 | null |
      | d | 38 | "" |
    When streams creation completes at millisecond 100 with error cache lifetime 1000 milliseconds
    Then all four streams topics leave the in-flight set
    When streams creation errors are queried at millisecond 1099
    Then the cached streams topic errors are JSON {"b":"Topic 'b' already exists.","c":"Configuration is invalid.","d":"Replication factor is below 1 or larger than the number of available brokers."}
    When streams creation errors are queried at millisecond 1100
    Then the cached streams topic errors are JSON {}

  Scenario: A controller timeout is cached for every topic
    Given the controller topic creation request timed out
    When streams creation completes at millisecond 100 with error cache lifetime 1000 milliseconds
    Then all four streams topics leave the in-flight set
    When streams creation errors are queried at millisecond 1099
    Then the cached streams topic errors are JSON {"a":"CreateTopicsRequest to controller timed out","b":"CreateTopicsRequest to controller timed out","c":"CreateTopicsRequest to controller timed out","d":"CreateTopicsRequest to controller timed out"}
    When streams creation errors are queried at millisecond 1100
    Then the cached streams topic errors are JSON {}

  Scenario: A refused controller envelope is cached for every topic
    Given the controller refused the envelope with CLUSTER_AUTHORIZATION_FAILED
    When streams creation completes at millisecond 100 with error cache lifetime 1000 milliseconds
    Then all four streams topics leave the in-flight set
    When streams creation errors are queried at millisecond 1099
    Then the cached streams topic errors are JSON {"a":"Cluster authorization failed.","b":"Cluster authorization failed.","c":"Cluster authorization failed.","d":"Cluster authorization failed."}
    When streams creation errors are queried at millisecond 1100
    Then the cached streams topic errors are JSON {}
