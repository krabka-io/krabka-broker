Feature: Consumer seeks apply across initial assignment and reject negative offsets
  Scenario: seek before first poll resumes from sought offset
    Given topic s has 5 records and an unpolled Earliest consumer in group seek-group
    When the consumer seeks partition 0 to offset 2
    Then the seek is accepted
    When the consumer polls until three records arrive or 15 seconds elapse
    Then exactly offsets 2 3 and 4 are delivered without pre-seek records

  Scenario: seek rejects negative offset
    Given topic n has 0 records and an unpolled Earliest consumer in group neg-group
    When the consumer seeks partition 0 to offset -1
    Then the seek is rejected
    When the consumer seeks partition 0 to offset 0
    Then the seek is accepted
