Feature: Streams subscriptions include source and repartition source topics

  Scenario Outline: Project the full subscribed topic set
    Given streams group membership and source/repartition topology <input>
    When OffsetDelete asks which topics the streams group subscribes to
    Then the complete subscribed topic set is <topics>

    Examples:
      | input | topics |
      | {"live":false,"topology":[["0","in",[],["rep"]],["1","other",["rep"],[]]]} | [] |
      | {"live":true,"topology":null} | [] |
      | {"live":true,"topology":[["0","in",[],["rep"]],["1","other",["rep"],[]]]} | ["in","other","rep"] |
