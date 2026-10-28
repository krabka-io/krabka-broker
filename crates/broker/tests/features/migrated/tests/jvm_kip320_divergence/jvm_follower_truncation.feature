Feature: KIP-320 truncation from Krabka leader to JVM follower

  Scenario: JVM truncates an equal-length divergent suffix and resumes consumption
    Given a mixed topic krabka-kip320-jvm-follower with a committed shared prefix
    When the promoted JVM leader appends divergent payloads ["jvm-divergent-0","jvm-divergent-1","jvm-divergent-2","jvm-divergent-3"]
    Then the JVM on-disk log contains divergent payload jvm-divergent-3
    When the paused JVM is isolated behind phantom leader 99 and Krabka replaces its suffix with 4 native records
    Then the Krabka rewrite starts at the shared prefix and adds exactly 4 offsets
    When Krabka becomes leader in the next epoch and the resumed JVM follower recovers
    Then the JVM log contains test-record-3 and excludes jvm-divergent- at the Krabka leader's exact end offset
    When Kafka console consumer reads from partition zero's beginning after recovery
    Then the consumer returns at least 1 nonempty records without failures ["LogTruncationException","RecordDeserializationException"]
    And the JVM divergence cluster is shut down
