Feature: Broker execution on a current-thread runtime
  Platforms without native threads must perform disk work inline. The ordinary
  executor is a control that proves the runtime's thread counter sees its pool.

  Scenario Outline: The blocking executor determines whether background threads are started
    Given blocking work uses <mode>
    When a current-thread broker creates produces and fetches records
    Then the broker serves three records and starts <threads> extra threads

    Examples:
      | mode               | threads |
      | the runtime thread | no      |
      | the blocking pool  | some    |
