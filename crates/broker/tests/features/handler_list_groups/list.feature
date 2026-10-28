Feature: ListGroups combines normalized state and type filters with authorization
  Scenario Outline: Filtering preserves complete rows and request-independent group order
    Given classic consumer share and streams groups span empty rebalancing reconciling stable and not-ready states with one denied group
    And ListGroups requests states <states> and types <types>
    When ListGroups filters the groups and applies group authorization
    Then the complete listed groups in order are <ids>
    Examples:
      | states                      | types           | ids                                                         |
      | none                        | none            | classic-a,classic-b,consumer-a,consumer-b,share-a,streams-a   |
      | empty                       | none            | classic-a,share-a                                            |
      | STABLE surrounded by spaces | none            | consumer-b                                                  |
      | reconciling,notready         | none            | consumer-a,streams-a                                        |
      | Dead                        | none            | none                                                        |
      | none                        | Share           | share-a                                                     |
      | none                        | CLASSIC,streams | classic-a,classic-b,streams-a                                |
      | none                        | unknown         | none                                                        |
      | empty                       | consumer        | none                                                        |
