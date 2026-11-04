Feature: Fencing policy refresh releases the leader high watermark
  Scenario Outline: A scan preserves durability while refreshing follower policy
    Given a <storage> leader whose recently caught-up follower has left the ISR
    When the scan refreshes policy while respecting current durability
    Then the high watermark is <watermark>, notification is <notified> and all-acks completion is <committed>
    Examples:
      | storage    | watermark | notified | committed |
      | synced     | 3         | true     | true      |
      | diskless   | 0         | false    | false     |
      | superseded | 0         | false    | false     |
      | expired       | 3 | true  | true  |
      | eligible      | 0 | false | false |
      | below-minimum | 0 | false | false |
      | concurrent-append | 4 | false | true |
      | pending-expansion | 0 | false | false |
      | uninstalled-isr | 0 | false | false |
