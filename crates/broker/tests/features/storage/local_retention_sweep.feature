Feature: Local retention trims every replica the broker hosts

  Rule: Retention deletes expired segments whoever leads the partition

    Scenario Outline: Segments past retention.ms leave the disk
      Given a <role> replica holds sealed segments past retention.ms
      When the local retention sweep runs
      Then the expired segments leave the disk

      Examples:
        | case                             | role     |
        | the leader trims its replica     | leader   |
        | a follower trims its own replica | follower |
