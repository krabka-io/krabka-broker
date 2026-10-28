Feature: Scheduled delivery holds future records and wakes long polls at activation
  Scenario Outline: a record stamped in the future waits for its delivery time
    Given topic <topic> has delivery mode <mode>
    When the producer appends due-soon stamped 2000 milliseconds in the future
    And a consumer reads ListOffsets LATEST and fetches from offset 0
    Then the visibility read completed before the delivery timestamp
    And LATEST is <latest> and fetched values are <values>
    When the consumer waits until due-soon is visible at LATEST 1
    Then first visibility is at or after the delivery timestamp <held>
    Examples:
      | mode | topic | latest | values | held |
      | scheduled | deliver-at-time-wait-scheduled | 0 | empty | true |
      | immediate | deliver-at-time-wait-immediate | 1 | due-soon | false |

  Scenario Outline: a parked long poll wakes when the record comes due
    Given topic <topic> has delivery mode <mode>
    When the producer appends due-soon stamped 2000 milliseconds in the future
    And the consumer starts a 20000 millisecond long poll for the record
    Then the long poll serves only due-soon within 10000 milliseconds
    And first visibility is at or after the delivery timestamp <held>
    Examples:
      | mode | topic | held |
      | scheduled | deliver-at-time-longpoll-scheduled | true |
      | immediate | deliver-at-time-longpoll-immediate | false |
