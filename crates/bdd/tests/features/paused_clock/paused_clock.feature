Feature: A scenario can run on a paused Tokio clock

  Scenario: An hour-long sleep finishes at once on the paused clock
    Given the paused clock has been read
    When a step sleeps for an hour
    Then an hour has passed on the paused clock
