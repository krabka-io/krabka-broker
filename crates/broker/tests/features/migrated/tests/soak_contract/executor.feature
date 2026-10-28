Feature: Recovery measurements depend on the actual fault and available evidence
  Scenario Outline: tests: a fault that lands on the controller is measured for its failover
    Given fault <kind> targets <target> and lands on a node holding role <role>
    When the executor selects recovery checks for the role the fault actually hit
    Then controller failover is measured exactly once: <measured>

    Examples:
      | example                                | kind               | target      | role       | measured |
      | leader restart on the controller       | ungraceful-restart | leader      | controller | true     |
      | leader restart on a leader             | ungraceful-restart | leader      | leader     | false    |
      | any failover pause on the controller   | failover-pause     | broker-zero | controller | true     |
      | quiet pause on the controller          | quiet-pause        | broker-zero | controller | false    |
      | controller restart measured once       | graceful-restart   | controller  | controller | true     |

  Scenario Outline: tests: a failover runs from the injection and a rejoin from the cleanup
    Given the recovery check is <check>
    When the executor chooses whether the recovery clock starts at injection
    Then the clock starts at injection rather than cleanup: <injection>

    Examples:
      | example                | check                 | injection |
      | leader failover        | leader-failover       | true      |
      | controller failover    | controller-failover   | true      |
      | isr rejoin             | isr-rejoin            | false     |
      | catch up               | catch-up              | false     |
      | certificate served     | certificate-served    | true      |
      | reassignment complete  | reassignment-complete | true      |

  Scenario Outline: tests: a leader failover is seen only against what the target is known to have led
    Given before injection node one led partition set <led>
    And metadata comes from node <from> with partition leader <leader>
    When the executor judges failover away from node one using only known metadata
    Then the observed leader recovery is <outcome>

    Examples:
      | example                              | led     | from | leader | outcome      |
      | unknown led is never failed over      | unknown | 2    | 2      | unmeasurable |
      | unknown led with no answer            | unknown | none | none   | unmeasurable |
      | nothing led has nothing to move       | none    | 2    | 1      | failed-over  |
      | led partition moved                   | 0       | 2    | 2      | failed-over  |
      | led partition still on the target     | 0       | 2    | 1      | waiting      |
      | only the target answers               | 0       | 1    | 2      | waiting      |
      | nobody answers                        | 0       | none | none   | waiting      |
