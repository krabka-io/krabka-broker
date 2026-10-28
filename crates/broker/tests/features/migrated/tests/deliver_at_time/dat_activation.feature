Feature: tests deliver at time dat activation

  Scenario: a record stamped in the future waits for its delivery time
    Then deliver at time dat activation: a record stamped in the future waits for its delivery time

  Scenario: a parked long poll wakes when the record comes due
    Then deliver at time dat activation: a parked long poll wakes when the record comes due
