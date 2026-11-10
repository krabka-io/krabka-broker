Feature: Classic group protocol negotiation

  Rule: Joining members must share an existing protocol and protocol type

    Scenario Outline: Joining members must share an existing protocol and protocol type
      Given the classic group is <state> with existing consumer protocol support
      When a joining member offers protocol type <kind> with preference list <names>
      Then the offered protocols are compatible <compatible>

      Examples:
        | case                                    | state  | kind     | names    | compatible |
        | an empty group needs a protocol type    | empty  | empty    | range    | false      |
        | an empty group needs a protocol         | empty  | consumer | none     | false      |
        | an empty group takes any protocol type  | empty  | connect  | x        | true       |
        | a member group keeps its protocol type  | stable | connect  | sticky   | false      |
        | a protocol no member supports           | stable | consumer | range    | false      |
        | one offered protocol every member has   | stable | consumer | x,sticky | true       |

  Rule: The group protocol is the shared protocol that most members prefer

    Scenario Outline: Each member votes for its most preferred protocol that every member supports
      Given the members of a classic group prefer the protocols <preferences>
      When the coordinator selects the group protocol
      Then the group protocol is <protocol>

      Examples:
        | case                                        | preferences                                                       | protocol           |
        | one member takes its first choice           | range,sticky                                                      | range              |
        | the majority of first choices wins          | range,sticky / range,sticky / sticky,range                        | range              |
        | first choices outside the intersection vote | roundrobin,sticky,range / cooperative,sticky,range / range,sticky | sticky             |
        | a tie goes to the alphabetically first name | range,cooperative_sticky / cooperative_sticky,range               | cooperative_sticky |

    Scenario Outline: A group with no protocol every member supports has no group protocol
      Given the members of a classic group prefer the protocols <preferences>
      When the coordinator selects the group protocol
      Then the group has no protocol

      Examples:
        | case               | preferences    |
        | no shared protocol | range / sticky |
        | no members         | none           |
