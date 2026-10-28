Feature: tests soak contract executor

  Scenario Outline: tests: a fault that lands on the controller is measured for its failover
    Then soak contract executor tests: a fault that lands on the controller is measured for its failover for <example>

    Examples:
      | example                              |
      | leader restart on the controller     |
      | leader restart on a leader           |
      | any failover pause on the controller |
      | quiet pause on the controller        |
      | controller restart measured once     |

  Scenario Outline: tests: a failover runs from the injection and a rejoin from the cleanup
    Then soak contract executor tests: a failover runs from the injection and a rejoin from the cleanup for <example>

    Examples:
      | example               |
      | leader failover       |
      | controller failover   |
      | isr rejoin            |
      | catch up              |
      | certificate served    |
      | reassignment complete |

  Scenario Outline: tests: a leader failover is seen only against what the target is known to have led
    Then soak contract executor tests: a leader failover is seen only against what the target is known to have led for <example>

    Examples:
      | example                           |
      | unknown led is never failed over  |
      | unknown led with no answer        |
      | nothing led has nothing to move   |
      | led partition moved               |
      | led partition still on the target |
      | only the target answers           |
      | nobody answers                    |
