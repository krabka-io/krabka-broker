# KIP-516: a request names a topic by its id, by its name, or by both. A
# request without an id carries the zero id. Each error is the code the
# broker reports for the offending topic or partition.
Feature: The broker resolves a requested topic under KIP-516's strict rules

  Rule: A topic id resolves the topic, and a name beside it must name the same topic

    Scenario: A topic id alone resolves its topic
      Given the cluster holds topic orders with id 7
      When a request resolves topic id 7 without a name
      Then the request resolves to topic orders

    Scenario: An unknown topic id fails
      Given the cluster holds topic orders with id 7
      When a request resolves topic id 99 without a name
      Then the request fails with UNKNOWN_TOPIC_ID

    Scenario: A topic id beside its own name resolves its topic
      Given the cluster holds topic orders with id 7
      When a request resolves topic id 7 named orders
      Then the request resolves to topic orders

    Scenario: A topic id beside a different name fails
      Given the cluster holds topic orders with id 7
      When a request resolves topic id 7 named payments
      Then the request fails with INCONSISTENT_TOPIC_ID

  Rule: A request without a topic id resolves the topic by its name

    Scenario: A known name resolves its topic
      Given the cluster holds topic orders with id 7
      When a request resolves topic name orders without an id
      Then the request resolves to topic orders

    Scenario: An unknown name fails
      Given the cluster holds topic orders with id 7
      When a request resolves topic name missing without an id
      Then the request fails with UNKNOWN_TOPIC_OR_PARTITION
