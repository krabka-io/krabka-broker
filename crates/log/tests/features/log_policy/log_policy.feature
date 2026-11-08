Feature: Kafka-compatible log storage policy

  Rule: A log without overrides takes the Kafka 4.x defaults

    Scenario: Every default matches Kafka 4.x and the documented storage policy
      When a log policy is created without overrides
      Then every setting matches Kafka 4.x and the documented storage policy

    Scenario: Default sizes and times keep their documented base units
      When a log policy is created without overrides
      Then sizes reach the storage layer in bytes and times in milliseconds

  Rule: A cleanup policy is a membership list of compact and delete

    Scenario Outline: Each cleanup policy reports its members and its Kafka text
      Given a log has cleanup policy <policy>
      When the cleanup policy is read back
      Then compact membership is <compact>
      And delete membership is <delete>
      And the Kafka text of the policy is <text>

      Examples:
        | case               | policy         | compact | delete | text           |
        | delete only        | delete         | false   | true   | delete         |
        | compact only       | compact        | true    | false  | compact        |
        | compact and delete | compact,delete | true    | true   | compact,delete |
        | no cleanup         | none           | false   | false  | empty          |
