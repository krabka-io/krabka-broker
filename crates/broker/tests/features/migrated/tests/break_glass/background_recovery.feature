Feature: Background unclean recovery honors the operator's configured policy

  Scenario Outline: background unclean recovery follows its configured rule
    Given a three-node broker cluster with background unclean recovery mode <mode>
    And RF3 topic recovered opts into strategy Aggressive
    When a non-controller broker dies and the partition names only that dead broker as leader and ISR
    Then a live recovery leader is elected <recovers> with offline observation window <window> ms
    And the unclean recovery bypass counter records an event <counted>

    Examples:
      | mode       | recovers | counted | window |
      | off        | true     | false   | 0      |
      | audit-only | true     | true    | 0      |
      | require    | false    | false   | 5000   |
