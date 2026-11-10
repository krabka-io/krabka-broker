Feature: New partitions are placed across the broker's log directories

  Rule: A partition that already has a directory stays in it

    Scenario: A partition found in the more loaded directory stays there
      Given partition t-0 already lives in log directory d2
      When the broker places partition t-0
      Then partition t-0 is placed in log directory d2

  Rule: A new partition goes to the directory with the fewest partitions and a tie goes to the first one

    Scenario Outline: The least loaded directory takes a new partition
      Given log directory d1 holds <first> partitions
      And log directory d2 holds <second> partitions
      When the broker places partition t-0
      Then partition t-0 is placed in log directory <chosen>

      Examples:
        | case                                     | first | second | chosen |
        | a tie goes to the first directory        | 0     | 0      | d1     |
        | one partition more sends it to the other | 1     | 0      | d2     |
        | the first directory is the less loaded   | 1     | 2      | d1     |

  Rule: A new partition avoids a cordoned directory unless every directory is cordoned

    Scenario: A new partition skips a cordoned directory that holds fewer partitions
      Given log directory d1 is cordoned
      And log directory d2 holds 1 partition
      When the broker places partition t-0
      Then partition t-0 is placed in log directory d2

    Scenario: A partition that lives in a cordoned directory stays there
      Given log directory d1 is cordoned
      And partition t-0 already lives in log directory d1
      When the broker places partition t-0
      Then partition t-0 is placed in log directory d1

    Scenario: With every directory cordoned a new partition goes to the first one
      Given every log directory is cordoned
      And log directory d1 holds 1 partition
      When the broker places partition t-0
      Then partition t-0 is placed in log directory d1
