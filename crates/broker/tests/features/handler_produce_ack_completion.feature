Feature: Produce acks all completion reads current metadata

  Scenario Outline: Durability and current metadata decide acknowledgement
    Given an empty orders partition with ISR <isr> and durability target <target>
    And the completion metadata lookup returns <minimum>
    When acks all completes at its deadline
    Then the full Produce row has error <error> and queries the admitted topic identity once

    Examples:
      | isr | target | minimum | error |
      | 1,2,3 | 0 | 2 | 0 |
      | 1,2 | 0 | 2 | 0 |
      | 1 | 0 | 2 | 20 |
      | 1,2 | 0 | 3 | 20 |
      | 1 | 0 | 1 | 0 |
      | 1,2,3 | 0 | recreated | 6 |
      | 1,2,3 | 0 | deleted | 3 |
      | 1,2,3 | 1 | 2 | 7 |
      | 1,2,3 | 1 | deleted | 3 |
