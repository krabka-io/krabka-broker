Feature: Fetch and ListOffsets preserve Kafka leader epoch fencing semantics
  Both APIs fence old epochs and reject future epochs. Fetch also accepts
  negative epochs beyond the no-epoch sentinel that ListOffsets recognizes.

  Background:
    Given a local partition is led at epoch 3

  Scenario Outline: Each API applies Kafka's leader epoch sentinel rule
    When Fetch and ListOffsets request current leader epoch <epoch>
    Then Fetch is <fetch> and ListOffsets is <list_offsets> against live epoch 3

    Examples:
      | epoch | fetch | list_offsets |
      | 2 | fenced | fenced |
      | 3 | allowed | allowed |
      | 4 | unknown | unknown |
      | -1 | allowed | allowed |
      | -2 | allowed | fenced |
      | -2147483648 | allowed | fenced |
