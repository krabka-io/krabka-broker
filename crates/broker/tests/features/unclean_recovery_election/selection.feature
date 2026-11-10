# KIP-966 unclean recovery polls the surviving replicas for their log state
# and elects one of the replicas that answered. A replica the partition still
# names in its ISR, then an eligible leader replica, holds every committed
# record and wins outright, as in Kafka's isValidNewLeader. Only when neither
# answered does the most complete log win, and only that election may lose
# data. A witness never leads.
Feature: Unclean recovery elects the surviving replica that loses the least

  Rule: Among replicas not known to be complete, the newest epoch wins, then the longest log, then the lowest broker id

    Scenario: A newer epoch beats a longer log
      Given these replicas answered the recovery poll
        | broker | last epoch | log end |
        | 2      | 4          | 100     |
        | 3      | 5          | 10      |
      When the controller elects a leader for the recovery
      Then broker 3 is elected as the most complete log

    Scenario: The longer log wins within an epoch
      Given these replicas answered the recovery poll
        | broker | last epoch | log end |
        | 2      | 5          | 90      |
        | 3      | 5          | 120     |
      When the controller elects a leader for the recovery
      Then broker 3 is elected as the most complete log

    Scenario: The lowest broker id breaks a full tie
      Given these replicas answered the recovery poll
        | broker | last epoch | log end |
        | 3      | 5          | 100     |
        | 1      | 5          | 100     |
        | 2      | 5          | 100     |
      When the controller elects a leader for the recovery
      Then broker 1 is elected as the most complete log

  Rule: A responder known to hold every committed record wins over a longer log, an ISR member first

    Scenario Outline: The ISR and the eligible leader replicas decide before the log length
      Given replicas 1, 2, 3 answered the recovery poll with log ends 40, 400, 20
      And the partition names ISR <isr> with eligible leader replicas <eligible>
      When the controller elects a leader for the recovery
      Then broker <leader> is elected as <basis>

      Examples:
        | case                                                 | isr  | eligible | leader | basis                       |
        | the shortest log wins as the only ISR member left    | 3    | none     | 3      | an in-sync replica          |
        | an ISR member beats an ELR member with a longer log  | 3    | 2        | 3      | an in-sync replica          |
        | the best ISR member wins when several answered       | 1,3  | 2        | 1      | an in-sync replica          |
        | an ISR member that did not answer decides nothing    | 9    | 3        | 3      | an eligible leader replica  |
        | the shortest log wins as the only ELR member         | none | 3        | 3      | an eligible leader replica  |
        | the best ELR member wins when several answered       | none | 1,3      | 1      | an eligible leader replica  |
        | an empty ELR falls back to the most complete log     | none | none     | 2      | the most complete log       |
        | an ELR member that did not answer cannot be elected  | none | 9        | 2      | the most complete log       |

  Rule: No rule elects a witness

    Scenario Outline: A witness among the responders
      Given replicas 1, 2, 3 answered the recovery poll with log ends 40, 400, 20
      And the partition names ISR <isr> with eligible leader replicas <eligible>
      And brokers <witnesses> are witnesses
      When the controller elects a leader for the recovery
      Then broker <leader> is elected as <basis>

      Examples:
        | case                                                   | isr  | eligible | witnesses | leader | basis                      |
        | the only ELR member is a witness                       | none | 3        | 3         | 2      | the most complete log      |
        | a data ELR member still outranks the longest log       | none | 1,3      | 3         | 1      | an eligible leader replica |
        | the fallback skips the witness with the longest log    | none | none     | 2         | 1      | the most complete log      |
        | an in-sync witness is still not elected                | 2    | none     | 2         | 1      | the most complete log      |

    Scenario: Nothing that may lead answered
      Given replicas 1, 2, 3 answered the recovery poll with log ends 40, 400, 20
      And the partition names ISR none with eligible leader replicas 3
      And brokers 1,2,3 are witnesses
      When the controller elects a leader for the recovery
      Then no leader is elected

  Rule: Without a response nobody is elected

    Scenario Outline: Nobody answered the recovery poll
      Given the partition names ISR <isr> with eligible leader replicas <eligible>
      When the controller elects a leader for the recovery
      Then no leader is elected

      Examples:
        | case                              | isr  | eligible |
        | nothing is known about the log    | none | none     |
        | the ELR names replicas            | none | 1,2      |
        | the ISR and the ELR name replicas | 1    | 2        |
