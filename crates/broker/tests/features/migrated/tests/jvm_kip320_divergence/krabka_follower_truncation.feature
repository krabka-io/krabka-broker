Feature: KIP-320 truncation from JVM leader to Krabka follower

  Scenario: Krabka removes its isolated suffix before following a promoted JVM leader
    Given a mixed topic krabka-kip320-krabka-follower with a committed shared prefix
    When replication is parked behind phantom leader 99 and Krabka appends 5 divergent records
    Then the parked Krabka log ends 5 offsets beyond the shared prefix
    When the JVM replica is promoted in the next epoch and Krabka recovers
    Then Krabka truncates exactly to the shared prefix and the JVM dump excludes test-record-
    When the JVM leader appends authoritative payloads ["jvm-authoritative-0","jvm-authoritative-1","jvm-authoritative-2"]
    Then the recovered Krabka log ends 3 offsets beyond the shared prefix
    And the reverse divergence cluster is shut down
