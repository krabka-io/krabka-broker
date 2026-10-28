Feature: src kraft controller tests fetch

  Scenario Outline: a refusal from the followed leader refetches as kafka does
    Then kraft controller tests fetch: a refusal from the followed leader refetches as kafka does for <example>

    Examples:
      | example                 |
      | not leader in its epoch |
      | fenced in its epoch     |
      | unknown leader epoch    |
      | older epoch             |
      | invalid request         |
      | from another node       |
      | following no one        |

  Scenario: an observer is tracked with kafkas caught up rule
    Then kraft controller tests fetch: an observer is tracked with kafkas caught up rule
