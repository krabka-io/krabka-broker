Feature: Native consumer routes Fetch to each partition leader

  Scenario: consumer fetches from non bootstrap leaders
    Given a three-broker topic routing-rf1 with six sole-replica partitions containing their pPARTITION values
    When an earliest-offset native consumer subscribes through broker one with group routing-grp-rf1
    Then records p0,p1,p2,p3,p4,p5 arrive, including a partition with no replica on the bootstrap broker
