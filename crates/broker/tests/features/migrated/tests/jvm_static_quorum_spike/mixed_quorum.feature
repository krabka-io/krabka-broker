Feature: Static mixed JVM and Krabka quorum
  Scenario: A JVM voter joins and replicates a Krabka-led static quorum
    Given two Krabka voters reserve a three-voter static quorum with shared cluster id 4d6b5533-4f45-5642-4e54-63774e544a45
    When Kafka JVM voter 3 starts and the mixed quorum is observed for 50 seconds
    Then the two Krabka voters observe a shared elected leader
    And the JVM joins as follower or leader and reports these replication signals without protocol failures
      | presence | log signal                                          |
      | present  | finished catching up to the current high water mark |
      | present  | metadata.version=25                                |
      | absent   | does not support VOTE                              |
      | absent   | UNSUPPORTED_VERSION                                |
      | absent   | Encountered fatal fault                            |
