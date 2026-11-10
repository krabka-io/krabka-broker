Feature: Consumer coordinator admission capacity and migration policy
  An operator who leaves the member limit unset can run a group larger than
  200 members, including a classic group upgrading to the consumer protocol.
  The group.consumer.migration.policy setting decides whether a classic group
  may become a consumer group, and the reverse.

  Rule: An unset member limit is Kafka's integer maximum

    Scenario: Consumer groups inherit Kafka's integer maximum member limit
      Given no consumer-group member limit has been overridden
      When the consumer coordinator reads its maximum group size
      Then the member limit is 2147483647 and permits more than 200 members

  Rule: The migration policy defaults to bidirectional, as in Kafka 4.0

    Scenario: A coordinator with no migration policy migrates both ways
      When the consumer coordinator starts without a migration policy
      Then the coordinator allows upgrades true and downgrades true

  Rule: Each migration policy allows exactly its own directions

    Scenario Outline: A migration policy name sets the directions a group may migrate in
      When an operator sets the migration policy to <name>
      Then the coordinator allows upgrades <upgrade> and downgrades <downgrade>

      Examples:
        | case                     | name          | upgrade | downgrade |
        | no migration             | disabled      | false   | false     |
        | classic to consumer only | upgrade       | true    | false     |
        | consumer to classic only | downgrade     | false   | true      |
        | both ways                | bidirectional | true    | true      |
        | the name ignores case    | BiDirectional | true    | true      |

  Rule: A name that is no migration policy is refused

    Scenario: An unknown migration policy is refused
      When an operator attempts to set the migration policy to sideways
      Then the policy is refused as an invalid migration policy

    Scenario: An empty migration policy is refused
      When an operator attempts to set an empty migration policy
      Then the policy is refused as an invalid migration policy
