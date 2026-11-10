Feature: JoinGroup member identifier bootstrap
  Scenario Outline: KIP-394 bootstraps only anonymous dynamic members at version four or later
    Given a JoinGroup request has member <member> and instance <instance>
    When the coordinator checks known member identity for JoinGroup version <version>
    Then the member must obtain an assigned member identifier <required>

    Examples:
      | case                               | member | instance   | version | required |
      | an anonymous dynamic member at v4  | empty  | absent     | 4       | true     |
      | an anonymous dynamic member at v3  | empty  | absent     | 3       | false    |
      | an anonymous static member         | empty  | instance-1 | 9       | false    |
      | a member that names its id         | m1     | absent     | 9       | false    |
