@docker
Feature: Stock Kafka tools administer brokers through the controller listener
  Scenario: Dynamic broker configuration round trips through the controller listener
    Given a broker exposes its live controller administration listener
    When the operator sets broker one's replication throttle and reads it back through the controller
    Then the successful controller config report contains the written replication throttle

  Scenario: ACL administration completes its lifecycle through the controller listener
    Given the live controller listener authorizes its ANONYMOUS principal as an ACL administrator
    When the controller ACL tool adds alice's Read binding and lists the topic
    Then the successful controller ACL listing contains alice's Read grant
    When the controller ACL tool removes alice's binding and lists the topic again
    Then the successful controller ACL listing no longer contains alice

  Scenario: Reassignment inspection reaches the controller listener
    Given a broker exposes its live controller administration listener
    When the operator lists reassignments through the controller listener
    Then the successful controller report says no partition reassignments exist

  Scenario: The stock topic tool explicitly lacks controller bootstrap support
    Given the pinned stock Kafka distribution provides the topic administration tool
    When the operator asks kafka topics to list through bootstrap controller
    Then the pinned topic tool reports that bootstrap controller is not a recognized option

  Scenario: Finalized features can be changed through the controller listener
    Given a broker exposes its live controller administration listener
    When the operator describes finalized features through the controller listener
    Then the successful controller report finalizes transaction version at 2
    When the operator downgrades transaction version to one through the controller listener
    And the operator describes finalized features through the controller listener
    Then the successful controller report finalizes transaction version at 1
