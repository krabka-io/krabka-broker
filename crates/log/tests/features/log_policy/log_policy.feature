Feature: Kafka-compatible log storage policy
  Scenario: Default log settings match the documented Kafka 4.x policy
    When the default log policy is requested
    Then all default log settings match Kafka 4.x and Krabka's documented storage policy

  Scenario: Dimensioned defaults retain their exact storage units
    When the default log policy is requested
    Then sizes and times cross the raw storage seams in their documented base units

  Scenario Outline: Cleanup policy text is a membership list
    Given log cleanup policy <policy>
    When cleanup policy membership and Kafka text are inspected
    Then compact membership is <compact> and delete membership is <delete>
    And cleanup policy text is <text>

    Examples:
      | policy         | compact | delete | text           |
      | delete         | false   | true   | delete         |
      | compact        | true    | false  | compact        |
      | compact,delete | true    | true   | compact,delete |
      | none           | false   | false  | empty          |
