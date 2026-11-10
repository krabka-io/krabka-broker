Feature: The range assignor gives each member contiguous partitions of each topic
  The server-side range assignor of KIP-848 keeps the classic RangeAssignor
  semantics, so topics with equal partition counts are co-partitioned.

  Rule: Subscribers split a topic in member id order, and the first members take the remainder

    Scenario Outline: A topic is split into contiguous ranges
      Given a topic orders with <partitions> partitions
      And members <members> subscribe to orders
      When the range assignor assigns the partitions
      Then the partitions of orders go to <owned>

      Examples:
        | case                                 | members  | partitions | owned                    |
        | an even split                        | m1,m2    | 6          | m1=0,1,2 m2=3,4,5        |
        | the first member takes the remainder | m1,m2,m3 | 7          | m1=0,1,2 m2=3,4 m3=5,6   |
        | fewer partitions than members        | m1,m2,m3 | 2          | m1=0 m2=1 m3=none        |

  Rule: Topics with equal partition counts are co-partitioned

    Scenario: Each member owns the same partitions of two equal topics
      Given topics orders,payments with 4 partitions each
      And members m1,m2 subscribe to orders,payments
      When the range assignor assigns the partitions
      Then the partitions of orders go to m1=0,1 m2=2,3
      And the partitions of payments go to m1=0,1 m2=2,3

  Rule: A member gets no partition of a topic it does not subscribe to

    Scenario: The only subscriber of a topic takes all of it
      Given a topic orders with 4 partitions
      And member m1 subscribes to orders
      And member m2 has no subscription
      When the range assignor assigns the partitions
      Then the partitions of orders go to m1=0,1,2,3 m2=none
