Feature: The scheduled delivery watermark stops at the first pending record
  Scenario Outline: list offsets latest lands a seek to end on the first pending record
    Given topic <topic> has delivery mode <mode>
    When the producer appends already-due values due-a,due-b stamped 60000 milliseconds in the past
    And the producer appends pending values pending stamped 3600000 milliseconds in the future
    And a consumer reads ListOffsets LATEST and fetches from offset 0
    Then LATEST is <latest> and fetched values are <values>
    Examples:
      | mode | topic | latest | values |
      | scheduled | deliver-at-time-latest-scheduled | 2 | due-a,due-b |
      | immediate | deliver-at-time-latest-immediate | 3 | due-a,due-b,pending |

  Scenario Outline: a batch scheduled far out holds back a later batch that is already due
    Given topic <topic> has delivery mode <mode>
    When the producer appends pending values pending stamped 3600000 milliseconds in the future
    And the producer appends already-due values already-due stamped 60000 milliseconds in the past
    And a consumer reads ListOffsets LATEST and fetches from offset 0
    Then LATEST is <latest> and fetched values are <values>
    Examples:
      | mode | topic | latest | values |
      | scheduled | deliver-at-time-head-of-line-scheduled | 0 | empty |
      | immediate | deliver-at-time-head-of-line-immediate | 2 | pending,already-due |
