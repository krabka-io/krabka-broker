Feature: tests flexible epoch value

  Scenario Outline: keeps exact kafka bytes and every truncation error
    Then flexible epoch value: keeps exact kafka bytes and every truncation error for <example>

    Examples:
      | example          |
      | zero timestamp   |
      | tagged timestamp |
      | negative values  |
