Feature: Replica reassignment completes only when Kafka membership rules permit
  In-flight replicas contain the target followed by removing replicas. A factor
  decrease waits for every target replica to join the ISR, while an equal or
  increased factor only waits for new additions.

  Scenario Outline: Complete reassignment when <case>
    Given the in-flight replicas, ISR, additions and removals are JSON <assignment>
    When the controller attempts to complete the replica reassignment
    Then the completed replica layout is JSON <completed>

    Examples:
      | case | assignment | completed |
      | no reassignment | {"replicas":[1,2,3],"isr":[1,2,3],"adding":[],"removing":[]} | null |
      | the additions are in the ISR | {"replicas":[1,2,3],"isr":[1,2,3],"adding":[3],"removing":[2]} | {"replicas":[1,3],"isr":[1,3]} |
      | an addition is not in the ISR | {"replicas":[1,2,3],"isr":[1,2],"adding":[3],"removing":[2]} | null |
      | the ISR would keep no member | {"replicas":[1,2,3],"isr":[3],"adding":[],"removing":[3]} | null |
      | a decrease with a target replica outside the ISR | {"replicas":[1,2,3],"isr":[1,3],"adding":[],"removing":[3]} | null |
      | a decrease with every target replica in the ISR | {"replicas":[1,2,3],"isr":[1,2,3],"adding":[],"removing":[3]} | {"replicas":[1,2],"isr":[1,2]} |
      | Kafka's example of a decrease that would shrink the ISR | {"replicas":[2,3,4,5,0,1],"isr":[0,1,5],"adding":[5],"removing":[0,1]} | null |
      | the same factor does not wait for the rest of the target | {"replicas":[2,3,4,1],"isr":[1,2,3],"adding":[3],"removing":[1]} | {"replicas":[2,3,4],"isr":[2,3]} |
      | an increase does not wait for the rest of the target | {"replicas":[1,2,3,4],"isr":[1,2,3],"adding":[3],"removing":[]} | {"replicas":[1,2,3,4],"isr":[1,2,3]} |
