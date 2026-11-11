@feature
Feature: Scenarios grouped by rules
  Background:
    Given a count contribution of 1

  Scenario: Outside a rule
    When the count increases by 2
    Then the count is 3

  @first
  Rule: The first rule adds ten
    Background:
      Given a count contribution of 10

    @plain
    Scenario: Inside the first rule
      When the count increases by 2
      Then the count is 13

    @outline
    Scenario Outline: Outline inside the first rule
      When the count increases by <amount>
      Then the count is <expected>

      @fast
      Examples:
        | amount | expected |
        | 1      | 12       |

      @slow
      Examples:
        | amount | expected |
        | 2      | 13       |

  @second
  Rule: The second rule adds twenty
    Background:
      Given a count contribution of 20

    Scenario: Inside the second rule
      When the count increases by 2
      Then the count is 23
