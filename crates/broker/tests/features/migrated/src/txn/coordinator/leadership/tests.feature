Feature: Transaction coordinator availability follows partition replay status

  Scenario Outline: Resolve coordinator availability from load status
    Given the transaction state partition load status is <status>
    When the transaction coordinator checks whether its partition can serve requests
    Then the transaction coordinator availability error is JSON <error>

    Examples:
      | status | error |
      | not led locally | 16 |
      | Pending | 14 |
      | Loading | 14 |
      | Loaded | null |
      | Failed | 16 |
