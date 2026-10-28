Feature: Audit topic placement belongs locally to each combined broker
  Scenario: every combined node leads one audit partition with a minimum isr of one
    Given 3 combined nodes with default minimum ISR 2 and gauge interval 100 milliseconds
    When the combined nodes start together and provision the audit topic
    Then each node leads its own single replica audit partition with effective minimum ISR 1 and no under-minimum partitions
