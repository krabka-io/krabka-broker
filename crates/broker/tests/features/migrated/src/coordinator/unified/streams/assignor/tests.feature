Feature: Streams assignment places concrete active and standby tasks across processes
  Scenario: Kafka task placement 1 no members
    Given subtopology s has 2 tasks stateful false and requests 0 standby replicas
    And Streams members have these processes owned tasks and reported task offsets
      | member | process | active | standby | warmup | offsets |
    When the Streams assignor places active and standby tasks in both member orders
    Then both member orders produce these complete active and standby task owners
      | member | active | standby |
  Scenario: Kafka task placement 2 empty group, stateless
    Given subtopology s has 4 tasks stateful false and requests 0 standby replicas
    And Streams members have these processes owned tasks and reported task offsets
      | member | process | active | standby | warmup | offsets |
      | A | p1 | {} | {} | {} | [] |
      | B | p2 | {} | {} | {} | [] |
    When the Streams assignor places active and standby tasks in both member orders
    Then both member orders produce these complete active and standby task owners
      | member | active | standby |
      | A | {"s":[0,2]} | null |
      | B | {"s":[1,3]} | null |
  Scenario: Kafka task placement 3 sticky within quota
    Given subtopology s has 4 tasks stateful true and requests 0 standby replicas
    And Streams members have these processes owned tasks and reported task offsets
      | member | process | active | standby | warmup | offsets |
      | A | p1 | {"s":[0,1,2,3]} | {} | {} | [] |
      | B | p2 | {} | {} | {} | [] |
    When the Streams assignor places active and standby tasks in both member orders
    Then both member orders produce these complete active and standby task owners
      | member | active | standby |
      | A | {"s":[0,1]} | null |
      | B | {"s":[2,3]} | null |
  Scenario: Kafka task placement 4 shouldAssignTasksToClientWithPreviousStandbyTasks
    Given subtopology s has 3 tasks stateful false and requests 0 standby replicas
    And Streams members have these processes owned tasks and reported task offsets
      | member | process | active | standby | warmup | offsets |
      | member1 | process1 | {} | {"s":[2]} | {} | [] |
      | member2 | process2 | {} | {"s":[1]} | {} | [] |
      | member3 | process3 | {} | {"s":[0]} | {} | [] |
    When the Streams assignor places active and standby tasks in both member orders
    Then both member orders produce these complete active and standby task owners
      | member | active | standby |
      | member1 | {"s":[2]} | null |
      | member2 | {"s":[1]} | null |
      | member3 | {"s":[0]} | null |
  Scenario: Kafka task placement 5 shouldAssignTasksToClientWithPreviousWarmupTasks
    Given subtopology s has 12 tasks stateful true and requests 0 standby replicas
    And Streams members have these processes owned tasks and reported task offsets
      | member | process | active | standby | warmup | offsets |
      | member1 | process1 | {"s":[0,1,2]} | {} | {} | [] |
      | member2 | process2 | {"s":[3,4,5]} | {} | {} | [] |
      | member3 | process3 | {"s":[6,7]} | {} | {"s":[8]} | [] |
      | member4 | process4 | {"s":[9]} | {} | {} | [] |
    When the Streams assignor places active and standby tasks in both member orders
    Then both member orders produce these complete active and standby task owners
      | member | active | standby |
      | member1 | {"s":[0,1,2]} | null |
      | member2 | {"s":[3,4,5]} | null |
      | member3 | {"s":[6,7,8]} | null |
      | member4 | {"s":[9,10,11]} | null |
  Scenario: Kafka task placement 6 shouldAssignStandbyTaskToClientWithPreviousWarmupTaskOverLessLoadedClient
    Given subtopology s has 3 tasks stateful true and requests 1 standby replicas
    And Streams members have these processes owned tasks and reported task offsets
      | member | process | active | standby | warmup | offsets |
      | member1 | process1 | {"s":[0]} | {} | {} | [] |
      | member2 | process2 | {"s":[1]} | {} | {} | [] |
      | member3 | process3 | {"s":[2]} | {} | {"s":[1]} | [] |
      | member4 | process4 | {} | {} | {} | [] |
    When the Streams assignor places active and standby tasks in both member orders
    Then both member orders produce these complete active and standby task owners
      | member | active | standby |
      | member1 | {"s":[0]} | null |
      | member2 | {"s":[1]} | {"s":[0]} |
      | member3 | {"s":[2]} | {"s":[1]} |
      | member4 | null | {"s":[2]} |
  Scenario: Kafka task placement 7 shouldPreferMoreCaughtUpCandidateRegardlessOfPrevStandbyOrPrevWarmupRole
    Given subtopology s has 2 tasks stateful true and requests 1 standby replicas
    And Streams members have these processes owned tasks and reported task offsets
      | member | process | active | standby | warmup | offsets |
      | member1 | process1 | {"s":[0]} | {} | {} | [] |
      | member2 | process2 | {"s":[1]} | {} | {} | [] |
      | member3 | process3 | {} | {"s":[0]} | {} | [{"offset":100,"partition":0,"subtopology":"s"}] |
      | member4 | process4 | {} | {} | {"s":[0]} | [{"offset":10,"partition":0,"subtopology":"s"}] |
      | member5 | process5 | {} | {"s":[1]} | {} | [{"offset":10,"partition":1,"subtopology":"s"}] |
      | member6 | process6 | {} | {} | {"s":[1]} | [{"offset":100,"partition":1,"subtopology":"s"}] |
    When the Streams assignor places active and standby tasks in both member orders
    Then both member orders produce these complete active and standby task owners
      | member | active | standby |
      | member1 | {"s":[0]} | null |
      | member2 | {"s":[1]} | null |
      | member3 | null | {"s":[0]} |
      | member6 | null | {"s":[1]} |
  Scenario: Kafka task placement 8 shouldAssignStandbyToPreviousStandbyThatDoesNotHoldTheActiveTask
    Given subtopology s has 3 tasks stateful true and requests 1 standby replicas
    And Streams members have these processes owned tasks and reported task offsets
      | member | process | active | standby | warmup | offsets |
      | member1 | process1 | {} | {"s":[2]} | {} | [] |
      | member2 | process2 | {"s":[0]} | {"s":[2]} | {} | [] |
      | member3 | process3 | {"s":[1]} | {} | {} | [] |
      | member4 | process4 | {} | {} | {} | [] |
    When the Streams assignor places active and standby tasks in both member orders
    Then both member orders produce these complete active and standby task owners
      | member | active | standby |
      | member1 | {"s":[2]} | {"s":[0]} |
      | member2 | {"s":[0]} | {"s":[2]} |
      | member3 | {"s":[1]} | null |
      | member4 | null | {"s":[1]} |
  Scenario: Kafka task placement 9 shouldRankCurrentOwnersAheadOfReportedTaskOffsetsAndMostCaughtUpFirst
    Given subtopology s has 4 tasks stateful true and requests 0 standby replicas
    And Streams members have these processes owned tasks and reported task offsets
      | member | process | active | standby | warmup | offsets |
      | member1 | process1 | {"s":[0]} | {} | {} | [] |
      | member2 | process2 | {} | {"s":[0,1]} | {} | [] |
      | member3 | process3 | {} | {} | {} | [{"offset":1000000,"partition":0,"subtopology":"s"},{"offset":1000000,"partition":1,"subtopology":"s"},{"offset":100,"partition":2,"subtopology":"s"}] |
      | member4 | process4 | {} | {} | {} | [{"offset":50,"partition":2,"subtopology":"s"}] |
    When the Streams assignor places active and standby tasks in both member orders
    Then both member orders produce these complete active and standby task owners
      | member | active | standby |
      | member1 | {"s":[0]} | null |
      | member2 | {"s":[1]} | null |
      | member3 | {"s":[2]} | null |
      | member4 | {"s":[3]} | null |
  Scenario: Kafka task placement 10 shouldAssignStandbysAwayFromProcessHoldingStateOnColdStart
    Given subtopology s has 2 tasks stateful true and requests 1 standby replicas
    And Streams members have these processes owned tasks and reported task offsets
      | member | process | active | standby | warmup | offsets |
      | member1 | process1 | {} | {} | {} | [{"offset":100,"partition":0,"subtopology":"s"}] |
      | member2 | process2 | {} | {} | {} | [{"offset":100,"partition":1,"subtopology":"s"}] |
    When the Streams assignor places active and standby tasks in both member orders
    Then both member orders produce these complete active and standby task owners
      | member | active | standby |
      | member1 | {"s":[0]} | {"s":[1]} |
      | member2 | {"s":[1]} | {"s":[0]} |
  Scenario: Kafka task placement 11 shouldAssignTasksToProcessesReportingTaskOffsetsOnColdStartWithMultipleMembersPerProcess
    Given subtopology s has 4 tasks stateful true and requests 0 standby replicas
    And Streams members have these processes owned tasks and reported task offsets
      | member | process | active | standby | warmup | offsets |
      | member1_1 | process1 | {} | {} | {} | [{"offset":100,"partition":0,"subtopology":"s"},{"offset":100,"partition":1,"subtopology":"s"}] |
      | member1_2 | process1 | {} | {} | {} | [{"offset":100,"partition":0,"subtopology":"s"},{"offset":100,"partition":1,"subtopology":"s"}] |
      | member2_1 | process2 | {} | {} | {} | [{"offset":100,"partition":2,"subtopology":"s"},{"offset":100,"partition":3,"subtopology":"s"}] |
      | member2_2 | process2 | {} | {} | {} | [{"offset":100,"partition":2,"subtopology":"s"},{"offset":100,"partition":3,"subtopology":"s"}] |
    When the Streams assignor places active and standby tasks in both member orders
    Then both member orders produce these complete active and standby task owners
      | member | active | standby |
      | member1_1 | {"s":[0]} | null |
      | member1_2 | {"s":[1]} | null |
      | member2_1 | {"s":[2]} | null |
      | member2_2 | {"s":[3]} | null |
  Scenario: Kafka task placement 12 empty group, stateful, one standby
    Given subtopology s has 2 tasks stateful true and requests 1 standby replicas
    And Streams members have these processes owned tasks and reported task offsets
      | member | process | active | standby | warmup | offsets |
      | A | p1 | {} | {} | {} | [] |
      | B | p2 | {} | {} | {} | [] |
    When the Streams assignor places active and standby tasks in both member orders
    Then both member orders produce these complete active and standby task owners
      | member | active | standby |
      | A | {"s":[0]} | {"s":[1]} |
      | B | {"s":[1]} | {"s":[0]} |
  Scenario: Kafka task placement 13 orphan active goes to its previous standby
    Given subtopology s has 3 tasks stateful true and requests 1 standby replicas
    And Streams members have these processes owned tasks and reported task offsets
      | member | process | active | standby | warmup | offsets |
      | m2 | p2 | {"s":[1]} | {"s":[0]} | {} | [] |
      | m3 | p3 | {"s":[2]} | {"s":[1]} | {} | [] |
    When the Streams assignor places active and standby tasks in both member orders
    Then both member orders produce these complete active and standby task owners
      | member | active | standby |
      | m2 | {"s":[0,1]} | {"s":[2]} |
      | m3 | {"s":[2]} | {"s":[0,1]} |
  Scenario: Kafka task placement 14 one process gets no standby
    Given subtopology s has 2 tasks stateful true and requests 1 standby replicas
    And Streams members have these processes owned tasks and reported task offsets
      | member | process | active | standby | warmup | offsets |
      | A | p1 | {} | {} | {} | [] |
      | B | p1 | {} | {} | {} | [] |
    When the Streams assignor places active and standby tasks in both member orders
    Then both member orders produce these complete active and standby task owners
      | member | active | standby |
      | A | {"s":[0]} | null |
      | B | {"s":[1]} | null |
