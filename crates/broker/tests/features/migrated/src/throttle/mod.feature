Feature: Replica throttle lists accept Kafka list spellings

  Rule: Only the trimmed, non-empty entries of a list name replicas

    Scenario Outline: A list without pair entries selects no replica unless it is the wildcard
      Given the throttled replica configuration is JSON <value>
      When the broker attempts to parse the throttled replica list
      Then the list selects <selection>

      Examples:
        | case              | value | selection     |
        | an empty list     | ""    | no replica    |
        | a whitespace list | " "   | no replica    |
        | a padded wildcard | " * " | every replica |

    Scenario Outline: A list of pairs selects them in order
      Given the throttled replica configuration is JSON <value>
      When the broker attempts to parse the throttled replica list
      Then the pairs selected are <pairs>

      Examples:
        | case                          | value         | pairs         |
        | padded pairs keep their order | " 0:1 , 2:3 " | [[0,1],[2,3]] |
        | an empty entry between pairs  | "0:1,,1:2"    | [[0,1],[1,2]] |

  Rule: An entry is a partition:broker pair of unsigned decimal digits

    Scenario Outline: A malformed entry refuses the list
      Given the throttled replica configuration is JSON <value>
      When the broker attempts to parse the throttled replica list
      Then the list is refused because its only entry is not a pair

      Examples:
        | case                     | value        |
        | no separator             | "not-a-pair" |
        | a non-numeric broker     | "0:x"        |
        | a non-numeric partition  | "x:1"        |
        | a negative partition     | "-1:1"       |
        | an explicit plus sign    | "+0:1"       |
        | spaces around the colon  | "0 : 1"      |
