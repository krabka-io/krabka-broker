Feature: Follower catch-up credit and ISR maintenance
  Reaching the current end earns credit now; keeping pace with the previous end
  earns credit at the previous fetch. ISR shrink and expansion use that credit
  differently, while the leader remains selected and unassigned brokers leave.

  Scenario Outline: Fetch progress credits the current or previous fetch
    Given the leader log now ends at <current_end> after ending at <previous_end> on the previous fetch
    When the follower fetches offset <fetch_offset>
    Then the follower receives caught-up credit for <credit>

    Examples:
      | boundary                | current_end | previous_end | fetch_offset | credit             |
      | reaches current end     | 120         | 100          | 120          | this-fetch         |
      | reaches previous end    | 120         | 100          | 100          | the-previous-fetch |
      | below both ends         | 120         | 100          | 99           | neither-fetch      |
      | after leadership change | 120         | -1           | 0            | the-previous-fetch |

  Scenario: ISR maintenance keeps current members and admits recently active followers (1: boundary leader, role leader)
    Given an ISR member with role leader, matching log end false, recent catch-up false, and recent fetch false
    When the leader maintains ISR membership
    Then the member is selected for the ISR: true

  Scenario: ISR maintenance keeps current members and admits recently active followers (2: boundary reassigned away, role unassigned)
    Given an ISR member with role unassigned, matching log end true, recent catch-up true, and recent fetch true
    When the leader maintains ISR membership
    Then the member is selected for the ISR: false

  Scenario: ISR maintenance keeps current members and admits recently active followers (3: boundary idle at leader end, role in-sync)
    Given an ISR member with role in-sync, matching log end true, recent catch-up false, and recent fetch false
    When the leader maintains ISR membership
    Then the member is selected for the ISR: true

  Scenario: ISR maintenance keeps current members and admits recently active followers (4: boundary recently caught up, role in-sync)
    Given an ISR member with role in-sync, matching log end false, recent catch-up true, and recent fetch true
    When the leader maintains ISR membership
    Then the member is selected for the ISR: true

  Scenario: ISR maintenance keeps current members and admits recently active followers (5: boundary fetching behind too long, role in-sync)
    Given an ISR member with role in-sync, matching log end false, recent catch-up false, and recent fetch true
    When the leader maintains ISR membership
    Then the member is selected for the ISR: false

  Scenario: ISR maintenance keeps current members and admits recently active followers (6: boundary stopped fetching, role in-sync)
    Given an ISR member with role in-sync, matching log end false, recent catch-up false, and recent fetch false
    When the leader maintains ISR membership
    Then the member is selected for the ISR: false

  Scenario: ISR maintenance keeps current members and admits recently active followers (7: boundary fetched and caught up, role out-of-sync)
    Given an ISR member with role out-of-sync, matching log end false, recent catch-up true, and recent fetch true
    When the leader maintains ISR membership
    Then the member is selected for the ISR: true

  Scenario: ISR maintenance keeps current members and admits recently active followers (8: boundary catch-up credit too old, role out-of-sync)
    Given an ISR member with role out-of-sync, matching log end true, recent catch-up false, and recent fetch true
    When the leader maintains ISR membership
    Then the member is selected for the ISR: false

  Scenario: ISR maintenance keeps current members and admits recently active followers (9: boundary no recent fetch, role out-of-sync)
    Given an ISR member with role out-of-sync, matching log end false, recent catch-up true, and recent fetch false
    When the leader maintains ISR membership
    Then the member is selected for the ISR: false
