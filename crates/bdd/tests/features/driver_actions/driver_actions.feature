Feature: Steps manage topics through a registry driver

  Rule: An operator's topic changes reach the registry through named actions

    Scenario: A created topic is listed with its partitions
      When an operator creates the topic orders with 3 partitions
      Then the registry lists the topic orders with 3 partitions

    Scenario: A deleted topic is no longer listed
      Given a topic named orders has been created
      When an operator deletes the topic orders
      Then the registry lists no topics

  Rule: An action the registry refuses fails with the registry's reason

    Scenario: Deleting a topic that does not exist is refused
      When an operator attempts to delete the topic payments
      Then the deletion fails because the topic payments does not exist
