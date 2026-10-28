Feature: src kraft controller vote response tests

  Scenario Outline: a vote answer is read as kafka reads it
    Then kraft controller vote response tests: a vote answer is read as kafka reads it for <example>

    Examples:
      | example                               |
      | older epoch                           |
      | unknown leader epoch                  |
      | newer epoch with a leader             |
      | newer epoch fenced with a leader      |
      | newer epoch without a leader          |
      | newer epoch naming the asker          |
      | same epoch leader while none is known |
      | same epoch leader with not leader     |
      | same epoch leader while one is known  |
      | same epoch grant                      |
      | same epoch refusal                    |
      | same epoch error                      |
