Feature: StreamsGroupDescribe orders submitted and configured topologies in Kafka order
  Scenario Outline: Rendering preserves topology details while sorting subtopologies and topic lists
    Given members submit a <representation> topology at epoch 4 with reversed topic lists
      | id | sources | sinks | changelogs | repartitions |
      | b  | t2,t1   | s2,s1 | c2,c1      | r2,r1        |
      | a  | t2,t1   | s2,s1 | c2,c1      | r2,r1        |
    When StreamsGroupDescribe renders the submitted topology
    Then the complete topology orders subtopologies a,b and every topic list by name with <partitions> internal partitions
    Examples:
      | representation | partitions |
      | stored         | 0          |
      | configured     | 3          |
