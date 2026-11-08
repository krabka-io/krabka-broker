Feature: Steps refer back to the entities a scenario introduced

  Rule: The topic is the one introduced last

    Scenario: Partitions are added to the topic introduced last
      Given a topic named orders with 3 partitions
      And a topic named payments with 6 partitions
      When an operator adds 2 partitions to the topic
      Then the topic payments has 8 partitions

  Rule: A name finds its topic among several

    Scenario: Partitions are added to the topic named by the operator
      Given a topic named orders with 3 partitions
      And a topic named payments with 6 partitions
      When an operator adds 2 partitions to the topic orders
      Then the topic orders has 5 partitions

  Rule: A selector picks one segment out of several

    Scenario Outline: An operator inspects one segment of a partition
      Given a segment with base offset 0
      And a segment with base offset 100
      And a segment with base offset 250
      When an operator inspects the <selector> segment
      Then the inspected segment starts at offset <offset>

      Examples:
        | case            | selector | offset |
        | oldest segment  | oldest   | 0      |
        | latest segment  | latest   | 250    |
