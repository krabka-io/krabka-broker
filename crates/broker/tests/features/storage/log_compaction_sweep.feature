Feature: The log cleaner compacts every hosted replica of a compacted topic

  Rule: The cleaner compacts a partition whose cleanup.policy contains compact whoever leads it

    Scenario Outline: A replica of a compacted topic is compacted
      Given a <role> replica of a topic with cleanup.policy <policy>
      When the log cleaner sweeps the broker
      Then the superseded values leave the replica

      Examples:
        | case                                | role     | policy         |
        | the leader compacts its replica     | leader   | compact        |
        | a follower compacts its own copy    | follower | compact        |
        | compact and delete is still compact | leader   | compact,delete |

    Scenario: A replica of a delete-only topic is left alone
      Given a leader replica of a topic with cleanup.policy delete
      When the log cleaner sweeps the broker
      Then the replica keeps every record
      And the cleaner counts one sweep and no compaction

  Rule: A partition below min.cleanable.dirty.ratio waits until max.compaction.lag.ms has passed

    Scenario: A partition whose dirty share is below the ratio is not compacted
      Given a compacted replica with min.cleanable.dirty.ratio 1.0
      When the log cleaner sweeps the broker
      Then the replica keeps every record

    Scenario: An elapsed max.compaction.lag.ms forces a pass below the ratio
      Given a compacted replica with min.cleanable.dirty.ratio 1.0
      And its records are older than a max.compaction.lag.ms of 1 millisecond
      When the log cleaner sweeps the broker
      Then the superseded values leave the replica
