Feature: tests soak contract faults

  Scenario Outline: tests: a disk fill reaches its watermark or is refused
    Then soak contract faults tests: a disk fill reaches its watermark or is refused for <example>

    Examples:
      | example                   |
      | fills to the watermark    |
      | already past it           |
      | exactly the reserve left  |
      | refused below the reserve |

  Scenario: tests: a refusal says why
    Then soak contract faults tests: a refusal says why

  Scenario: tests: a dropped guard undoes its fault
    Then soak contract faults tests: a dropped guard undoes its fault

  Scenario: tests: a cleared guard reports its cleanup
    Then soak contract faults tests: a cleared guard reports its cleanup
