Feature: Operators trigger a cut on demand
  Triggering injects an epoch-stamped marker into every partition of a
  group's topics and prints the offset each marker took. Those offsets are
  the cut. KFC-4 is the specification:
  docs/KFCs/KFC-4-cross-topic-snapshots.md.

  Rule: A cut names an offset for every partition of every topic in the group

    Scenario: A cut across orders and payments names all five partitions
      Given the orders topic has 3 partitions
      And the payments topic has 2 partitions
      And an operator has defined a barrier group over the orders and payments topics
      When the operator triggers a cut of the group
      Then the cut names an offset for exactly these partitions:
        | topic    | partition |
        | orders   | 0         |
        | orders   | 1         |
        | orders   | 2         |
        | payments | 0         |
        | payments | 1         |

  Rule: Epochs count up from 1, one per cut

    Scenario: The first cut of a group is epoch 1
      Given an operator has defined a barrier group over the orders topic
      When the operator triggers a cut of the group
      Then the cut is epoch 1

    Scenario: The cut after two others is epoch 3
      Given a barrier group over the orders topic has been cut 2 times
      When the operator triggers a cut of the group
      Then the cut is epoch 3

  Rule: A topic is resolved at each cut, not when the group is defined

    Scenario: A topic that does not exist contributes no partition to a cut
      Given the orders topic has 2 partitions
      And an operator has defined a barrier group over the orders and payments topics
      When the operator triggers a cut of the group
      Then the cut names an offset for exactly these partitions:
        | topic  | partition |
        | orders | 0         |
        | orders | 1         |

    Scenario: A topic created after the group was defined is in the next cut
      Given an operator has defined a barrier group over the payments topic
      And the payments topic was created afterwards with 2 partitions
      When the operator triggers a cut of the group
      Then the cut names an offset for exactly these partitions:
        | topic    | partition |
        | payments | 0         |
        | payments | 1         |

  Rule: Only a defined group can be cut

    @inprogress
    Scenario: A cut of a group nobody defined is refused
      When an operator tries to trigger a cut of a group nobody defined
      Then the broker refuses the request as an unknown group
