Feature: Ordinary transaction timeouts obey Kafka positive bounded milliseconds

  Background:
    Given the maximum ordinary transaction timeout is 8000 milliseconds

  Scenario Outline: Accept a timeout within the positive configured range
    Given the producer requests transaction timeout <requested> milliseconds
    When the broker resolves the non-two-phase-commit transaction timeout
    Then the configured ordinary transaction timeout is <requested> milliseconds

    Examples:
      | requested |
      | 1 |
      | 5000 |
      | 8000 |

  Scenario Outline: Refuse a timeout outside the Kafka range
    Given the producer requests transaction timeout <requested> milliseconds
    When the broker resolves the non-two-phase-commit transaction timeout
    Then the transaction timeout is refused with Kafka error code 50

    Examples:
      | requested |
      | 8001 |
      | 0 |
      | -1 |
      | -2147483648 |
      | 2147483647 |
