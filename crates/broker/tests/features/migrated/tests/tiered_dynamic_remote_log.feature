Feature: Remote-log quota changes take effect without restarting the broker

  Background:
    Given a broker copies to a local archive every second

  Scenario: the remote fetch quota holds back cold reads until it is raised
    When topic tiered-fetch-quota receives 80 records with 1-KiB segments
    And local retention evicts offset zero so that only the archive holds it
    Then the cold fetch serves offset 0 with HWM and LSO 80, log start 0, no error and no aborted-transaction list
    When the broker sets the remote fetch quota to 1 byte per second
    Then the cold fetch returns no records with HWM and LSO 80, log start 0, no error and no aborted-transaction list
    When the broker deletes the remote fetch quota override
    Then the cold fetch serves offset 0 with HWM and LSO 80, log start 0, no error and no aborted-transaction list

  Scenario: the remote copy quota holds back uploads until it is raised
    When the broker sets the remote copy quota to 1 byte per second
    And topic tiered-copy-quota receives 80 records with 1-KiB segments
    Then the archive holds only the first segment after five further copy ticks
    When the broker deletes the remote copy quota override
    Then the archive eventually holds at least three segments
