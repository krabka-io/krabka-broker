Feature: JVM consumers retain tiered records through disable, metadata restart and archive-only restore

  Scenario: tiered storage round trip through minio
    Given a readable MinIO tier with in-memory metadata has 200 records in topic krabka-tiered-minio-itest
    Then MinIO contains at least two sealed segment log objects
    When the JVM consumer reads up to 200 records from offset 0 with a 20000-millisecond timeout
    Then the JVM consumer returns at least 200 records through the remote tier

  Scenario: tiered storage disable needs delete on disable
    Given a readable MinIO tier with in-memory metadata has 200 records in topic krabka-tiered-disable-itest
    Then MinIO contains at least two sealed segment log objects
    When kafka-configs alters the topic with remote.storage.enable=false
    Then the bare disable fails and names both remote.log.delete.on.disable=true and remote.log.copy.disable=true
    When kafka-configs alters the topic with remote.storage.enable=false,remote.log.delete.on.disable=true
    Then the disable with delete-on-disable succeeds
    And within thirty seconds the bucket no longer contains the topic and its global log start is greater than zero

  Scenario: tiered storage topic rlmm survives restart
    Given a readable MinIO tier has 200 records in topic krabka-tiered-restart-itest with five RF1 metadata partitions, min ISR1, two-second snapshots and auto-derived bootstrap and snapshot directory
    Then MinIO contains at least two sealed segment log objects
    When three seconds allow a metadata snapshot and the broker shuts down then rejoins on the same log directory
    And the JVM console consumer reads 200 records from the restarted broker's partition 0 beginning with a 30000-millisecond timeout
    Then the restarted consumer returns at least 200 records including record-0000,0001,0050,0100,0150,0199

  Scenario: restored cluster serves the jvm console consumer
    Given a readable MinIO tier with in-memory metadata has 200 records in topic krabka-tiered-restore-itest
    Then MinIO contains at least two complete segments and the copy task has no remaining work
    When the source broker shuts down and its entire local log directory is discarded
    And the restore command reads only the MinIO archive into a new standalone log directory
    Then the restore recovers between 1 and 200 archived records, excluding any untiered active tail
    When a new broker boots on the restore output and the JVM consumer reads exactly the archived record count from beginning
    Then the consumer returns the archive's full record count in uninterrupted record-0000 order
