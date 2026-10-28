Feature: Kafka election retry jitter
  Election retries wait for at least the configured timeout and less than twice
  that timeout. Different voters must not remain synchronized across epochs.

  Scenario: Election retries are bounded and staggered across voters
    Given five voters with an epoch-one log and a 1000 ms election timeout
    When every voter times out in each of the first 64 epochs
    Then every new election deadline is at least 1000 ms and less than 2000 ms away
    And the voters draw more than 64 distinct deadlines across those epochs
