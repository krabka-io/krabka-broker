Feature: Preserve acknowledged diskless offsets through a crash during live flushing

  Scenario: a broker crashed mid flush loses no acknowledged offset
    Given a diskless quorum with 24 acknowledged records and a 50 millisecond flush cadence
    When a whole flush cycle passes the prefix under live churn before the accepting broker crashes
    Then the surviving leader serves all 24 acknowledged offsets with the produced payloads
    When the crashed broker restarts on its original addresses and rejoins all three registrations
    Then the restarted cluster serves all 24 acknowledged offsets in exactly the post-crash batch bytes
    And the cycling-flush recovery cluster is shut down
