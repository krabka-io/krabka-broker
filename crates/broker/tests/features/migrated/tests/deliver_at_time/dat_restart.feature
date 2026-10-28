Feature: Scheduled delivery derives the same watermark from recovered batch timestamps
  Scenario Outline: the delivery watermark survives a broker restart
    Given topic <topic> has delivery mode <mode>
    When the producer appends already-due values due stamped 60000 milliseconds in the past
    And the producer appends pending values pending stamped 3600000 milliseconds in the future
    And a consumer reads ListOffsets LATEST and fetches from offset 0
    Then LATEST is <latest> and fetched values are <values>
    When the broker shuts down and restarts from the same log directory
    And a consumer reads ListOffsets LATEST and fetches from offset 0
    Then LATEST is <latest> and fetched values are <values>
    Examples:
      | mode | topic | latest | values |
      | scheduled | deliver-at-time-restart-scheduled | 1 | due |
      | immediate | deliver-at-time-restart-immediate | 2 | due,pending |
