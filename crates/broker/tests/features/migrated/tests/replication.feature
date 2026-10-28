Feature: Followers recover replicated logs and batch their fetch sessions

  Scenario: replication factor three propagates to all followers
    Given a three-replica topic repl assigns brokers one, two, and three
    When the leader appends 20 records in batches of 20
    Then every replica's local log reaches end offset 20

  Scenario: out of range at a batch boundary restarts at the leader log start
    Given a three-replica topic oor assigns brokers one, two, and three
    When the leader appends 50 records in batches of 1
    Then every replica's local log reaches end offset 50
    When broker three loses its local log and the leader advances its log start to twenty-five
    Then broker three recovers to end fifty with log start twenty-five and batch bases 25..50

  Scenario: out of range inside a batch restarts at the batch base
    Given a three-replica topic oor-mid-batch assigns brokers one, two, and three
    When the leader appends 50 records in batches of 10
    Then every replica's local log reaches end offset 50
    When broker three loses its local log and the leader advances its log start to twenty-five
    Then broker three recovers to end fifty with log start twenty-five and batch bases 20,30,40

  Scenario: a follower that catches up across segments copies every batch
    Given topic seams has replicas one and two with one-MiB segments in a registered three-broker cluster
    When the seams leader appends twelve separate batches with 300000-byte records
    Then broker two's local log reaches end offset twelve
    When broker two's local log is truncated to zero
    Then leader and follower both contain every batch base zero through eleven

  Scenario: delete records moves every replica log start before it answers
    Given a three-replica topic trimmed assigns brokers one, two, and three
    When the leader appends 20 records in batches of 20
    Then every replica's local log reaches end offset 20
    When DeleteRecords trims the replicated partition to offset ten
    Then DeleteRecords returns exactly partition zero at low watermark ten without error
    And all three replicas already report log start ten without waiting

  Scenario: a follower batches every partition of one leader into one session
    Given a metrics-enabled three-broker cluster replicates twelve partitions of topic batched
    When incremental replication session count is observed for up to twenty seconds
    Then the leader holds exactly two incremental fetch sessions for its two followers

  Scenario: a caught up follower fetches a few times a second whatever it follows
    Given a metrics-enabled three-broker cluster replicates twelve partitions of topic idle-rate
    When followers settle for two seconds and Fetch requests are sampled over three seconds
    Then the caught-up leader receives fewer than forty Fetch requests per second
