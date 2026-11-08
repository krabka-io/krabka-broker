Feature: A session expires when its timeout has passed

  Rule: A session expires once its whole timeout has passed

    Scenario Outline: A session is checked as time passes
      Given a session with a timeout of 5 seconds has been opened
      When <elapsed> seconds pass
      Then the session is <state>

      Examples:
        | case                    | elapsed | state   |
        | before the deadline     | 4       | live    |
        | exactly at the deadline | 5       | expired |
        | past the deadline       | 6       | expired |
