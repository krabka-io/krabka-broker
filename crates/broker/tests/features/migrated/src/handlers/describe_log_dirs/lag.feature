Feature: src handlers describe log dirs lag

  Scenario: tests: offset lag matches kafka
    Then handlers describe log dirs lag tests: offset lag matches kafka

  Scenario Outline: tests: future offset lag matches kafka
    Then handlers describe log dirs lag tests: future offset lag matches kafka for <example>

    Examples:
      | example              |
      | no local log         |
      | three records behind |
      | future ahead         |
