Feature: One feature runs against every driver

  Rule: A created topic is listed

    Scenario: A topic created through the driver is listed
      Given a topic named orders has been created
      When the topics are listed
      Then the listing holds orders

  Rule: The wire reports the protocol it spoke

    @only_wire
    Scenario: The wire driver lists topics over the Kafka protocol
      When the topics are listed
      Then the listing came over the Kafka protocol
