Feature: JoinGroup member identifier bootstrap
  Scenario Outline: KIP-394 bootstraps only anonymous dynamic members at version four or later
    Given a JoinGroup request has member <member> and instance <instance>
    When the coordinator checks known member identity for JoinGroup version <version>
    Then the member must obtain an assigned member identifier <required>
    Examples:
      | member | instance | version | required |
      | empty | absent | 4 | true |
      | empty | absent | 9 | true |
      | empty | absent | 3 | false |
      | empty | instance-1 | 9 | false |
      | m1 | absent | 9 | false |
