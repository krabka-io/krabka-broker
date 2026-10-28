Feature: tests consumer group next gen persistence

  Scenario: replay preserves group epoch and members
    Then consumer group next gen persistence: replay preserves group epoch and members

  Scenario: next gen state cleared after leave then restart
    Then consumer group next gen persistence: next gen state cleared after leave then restart

  Scenario: replay keeps the topics a regex resolved to and finds new ones
    Then consumer group next gen persistence: replay keeps the topics a regex resolved to and finds new ones
