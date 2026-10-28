Feature: Classic group protocol negotiation
  Scenario Outline: Joining members must share an existing protocol and protocol type
    Given the classic group is <state> with existing consumer protocol support
    When a joining member offers protocol type <kind> with preference list <names>
    Then the offered protocols are compatible <compatible>
    Examples:
      | state | kind | names | compatible |
      | empty | empty | range | false |
      | empty | consumer | none | false |
      | empty | connect | x | true |
      | stable | connect | sticky | false |
      | stable | consumer | range | false |
      | stable | consumer | x,sticky | true |
