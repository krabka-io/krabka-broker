# The controller takes a failed broker out of the ISR of each partition it is
# in, and re-elects only where the leader is no longer a valid leader of what
# is left, taking the first live replica of that ISR in assignment order. When
# no live ISR member is left, the eligible leader replicas of KIP-966, the
# unclean election of KIP-841 or unclean recovery decide. A witness
# replicates the partition but never leads it.
Feature: A broker failure moves only the partitions Kafka's handleBrokerFenced moves
  Depends on: Leaderless partitions follow Kafka's last-known and eligible replica election ladder - ../policy.feature

  Rule: A failed broker leaves only its own place in the ISR, and only an invalid leader is replaced

    Scenario Outline: A failure of one broker of a partition
      Given a partition with replicas <replicas> led by broker <leader> with ISR <isr>
      When broker <failed> fails while brokers <alive> stay alive
      Then the controller decides to <decision>

      Examples:
        | case                                                 | leader | replicas | isr   | failed | alive | decision                            |
        | a failed follower leaves the ISR under a live leader | 1      | 1,2,3    | 1,2,3 | 3      | 1,2   | shrink the ISR to 1,2               |
        | a live ISR member replaces a down leader             | 1      | 1,2,3    | 1,2,3 | 3      | 2     | elect broker 2 cleanly with ISR 1,2 |
        | the first live replica in assignment order leads     | 1      | 1,2,3    | 1,2,3 | 1      | 2,3   | elect broker 2 cleanly with ISR 2,3 |
        | a down follower keeps its place for its own failure  | 1      | 1,2,3    | 1,2,3 | 1      | 2     | elect broker 2 cleanly with ISR 2,3 |
        | the clean pick walks the assignment, not the ISR     | 1      | 1,3,2    | 1,2,3 | 1      | 2,3   | elect broker 3 cleanly with ISR 2,3 |
        | a failed replica outside the ISR changes nothing     | 1      | 1,2,3    | 1,2   | 3      | 1     | leave the partition unchanged       |
        | a broker that holds no replica changes nothing       | 1      | 1,2,3    | 1,2,3 | 9      | 1,2,3 | leave the partition unchanged       |

  Rule: With no live ISR member left, only the unclean election or unclean recovery elects a leader

    Scenario Outline: A failure that leaves no live ISR member
      Given a partition with replicas 1,2,3 led by broker <leader> with ISR <isr>
      And the topic's recovery policy is <policy>
      When broker <failed> fails while brokers <alive> stay alive
      Then the controller decides to <decision>

      Examples:
        | case                                               | leader | isr | failed | alive | policy            | decision                                        |
        | a clean-only topic stays unavailable               | 1      | 1   | 1      | 2,3   | clean only        | mark the partition unavailable                  |
        | the unclean election takes the first live replica  | 1      | 1   | 1      | 2,3   | unclean election  | elect broker 2 uncleanly                        |
        | balanced recovery takes the partition over         | 1      | 1   | 1      | 2,3   | balanced recovery | hand the partition to balanced unclean recovery |
        | a down leader with nothing to elect keeps the rest | 2      | 2,3 | 3      | 1     | clean only        | shrink the ISR to 2                             |
        | the unclean election elects outside the ISR        | 2      | 2,3 | 3      | 1     | unclean election  | elect broker 1 uncleanly                        |

    Scenario: A follower failure that no live broker survives goes to unclean recovery
      Given a partition with replicas 1,2,3 led by broker 1 with ISR 1,2,3
      And the topic's recovery policy is balanced recovery
      When broker 3 fails while no broker is known to be alive
      Then the controller decides to hand the partition to balanced unclean recovery

  Rule: A witness never leads, and stays in the ISR it keeps writable

    Scenario Outline: A failure where a witness could take the lead
      Given a partition with replicas 1,2,3 led by broker 1 with ISR <isr>
      And broker 2 is a witness
      And the topic's recovery policy is <policy>
      When broker <failed> fails while brokers <alive> stay alive
      Then the controller decides to <decision>

      Examples:
        | case                                                 | isr   | failed | alive | policy              | decision                            |
        | the clean pick skips a witness that sorts first      | 1,2,3 | 1      | 2,3   | clean only          | elect broker 3 cleanly with ISR 2,3 |
        | a follower failure keeps the witness in the ISR      | 1,2,3 | 3      | 1,2   | clean only          | shrink the ISR to 1,2               |
        | a witness alone is no clean leader                   | 1,2,3 | 1      | 2     | clean only          | mark the partition unavailable      |
        | a witness alone is no unclean leader                 | 1,2,3 | 1      | 2     | unclean election    | mark the partition unavailable      |
        | a witness alone is not handed to balanced recovery   | 1,2,3 | 1      | 2     | balanced recovery   | mark the partition unavailable      |
        | a witness alone is not handed to aggressive recovery | 1,2,3 | 1      | 2     | aggressive recovery | mark the partition unavailable      |
        | the unclean election skips the witness               | 1     | 1      | 2,3   | unclean election    | elect broker 3 uncleanly            |
        | an unclean election with only a witness left         | 1     | 1      | 2     | unclean election    | mark the partition unavailable      |

  Rule: Once the live ISR is empty, a live eligible leader replica is elected cleanly whatever the recovery policy

    Scenario Outline: The leader of a one-member ISR fails beside an eligible leader replica
      Given a partition with replicas 1,3,2 led by broker 1 with ISR 1, whose eligible leader replicas are 2
      And the topic's recovery policy is <policy>
      When broker 1 fails while brokers 2,3 stay alive
      Then the controller decides to elect broker 2 cleanly with ISR 2

      Examples:
        | case                               | policy              |
        | it saves a clean-only topic        | clean only          |
        | it outranks the first live replica | unclean election    |
        | balanced recovery is not needed    | balanced recovery   |
        | aggressive recovery is not needed  | aggressive recovery |

    Scenario: A live ISR decides the election and the eligible leader replicas do not
      Given a partition with replicas 1,2,3 led by broker 1 with ISR 1,2,3, whose eligible leader replicas are 3
      And the topic's recovery policy is unclean election
      When broker 1 fails while brokers 2,3 stay alive
      Then the controller decides to elect broker 2 cleanly with ISR 2,3

    Scenario: A down eligible leader replica leaves the unclean election to decide
      Given a partition with replicas 1,2,3 led by broker 1 with ISR 1, whose eligible leader replicas are 2
      And the topic's recovery policy is unclean election
      When broker 1 fails while broker 3 stays alive
      Then the controller decides to elect broker 3 uncleanly

    Scenario: A witness among the eligible leader replicas leaves the unclean election to decide
      Given a partition with replicas 1,2,3 led by broker 1 with ISR 1, whose eligible leader replicas are 2
      And broker 2 is a witness
      And the topic's recovery policy is unclean election
      When broker 1 fails while brokers 2,3 stay alive
      Then the controller decides to elect broker 3 uncleanly

  Rule: A broker back from an unclean shutdown leaves only the ISR, whatever the controller thinks of its liveness

    Scenario Outline: A broker re-registers after an unclean shutdown
      Given a partition with replicas 1,2,3 led by broker <leader> with ISR <isr>
      When broker 3 returns from an unclean shutdown while brokers 1,2 stay alive
      Then the controller decides to <decision>

      Examples:
        | case                                          | leader | isr   | decision                            |
        | a returning leader is failed over             | 3      | 1,2,3 | elect broker 1 cleanly with ISR 1,2 |
        | a broker outside the ISR has nothing to leave | 1      | 1,2   | leave the partition unchanged       |

    Scenario: A new controller that has seen no broker alive still only shrinks the ISR
      Given a partition with replicas 1,2,3 led by broker 1 with ISR 1,2,3
      And the topic's recovery policy is balanced recovery
      When broker 3 returns from an unclean shutdown before any broker is known to be alive
      Then the controller decides to shrink the ISR to 1,2

    Scenario: A failed leader stays in the target ELR and keeps the last-known rung shut
      Given a partition with replicas 1,2,3 led by broker 1 with ISR 1, whose last known eligible leader replica is 2
      When broker 1 fails while brokers 2,3 stay alive
      Then the controller decides to mark the partition unavailable

    Scenario: A leader back from an unclean shutdown stays out of the target ELR
      Given a partition with replicas 1,2,3 led by broker 1 with ISR 1, whose last known eligible leader replica is 2
      When broker 1 returns from an unclean shutdown while brokers 2,3 stay alive
      Then the controller decides to elect broker 2 uncleanly
