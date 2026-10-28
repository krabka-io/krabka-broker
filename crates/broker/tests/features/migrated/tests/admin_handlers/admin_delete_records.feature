Feature: Trimming records durably within an active log segment

  Scenario: delete records trims log start
    Given an admin client connected to one standalone broker
    And topic t-dr contains 100 records on the admin broker
    When DeleteRecords trims the topic before offset 50
    Then the successful trim reports a valid low watermark equal to the local log start

  Scenario: a trim inside the active segment survives a restart
    Given a restartable topic contains 100 committed records entirely in segment 0
    When DeleteRecords trims the topic before offset 50
    Then the live trim succeeds and advances both low watermark and log start to 50
    When the trimmed broker shuts down
    Then the partition still has only segment 0 to witness the trim
    When the broker restarts from that same log directory
    And the client asks for the earliest offset and fetches from offset 0
    Then the restarted log begins at 50 and refuses the old fetch without serving records
