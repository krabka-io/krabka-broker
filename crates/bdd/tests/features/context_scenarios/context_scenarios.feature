Feature: Each scenario has its own scenario context

  Rule: A scenario sees only the state its own steps set

    Scenario: Records appended to a three-partition topic are counted once
      Given a topic with 3 partitions
      When a producer appends 2 records
      Then the topic holds 2 records across 3 partitions

    Scenario: Records appended to a one-partition topic are counted once
      Given a topic with 1 partition
      When a producer appends 5 records
      Then the topic holds 5 records across 1 partition
