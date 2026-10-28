Feature: Both append paths write indexes that Kafka dump-log verifies
  Scenario: The encoding append path retains Kafka index batch boundaries
    Given log batches for Kafka index verification
      | base | records | timestamp |
      | 0    | 3       | 100       |
      | 3    | 1       | 50        |
      | 4    | 2       | 200       |
      | 6    | 1       | 201       |
      | 7    | 1       | 300       |
    When the batches are appended through the encoding append path
    Then the offset index names batch ends 3,5,6,7
    And the time index names these running maxima and batch ends
      | timestamp | offset |
      | 102       | 2      |
      | 201       | 5      |
      | 300       | 7      |
    And Kafka dump-log index validation finds no problems

  Scenario: The verbatim append path retains Kafka index batch boundaries
    Given log batches for Kafka index verification
      | base | records | timestamp |
      | 0    | 3       | 100       |
      | 3    | 1       | 50        |
      | 4    | 2       | 200       |
      | 6    | 1       | 201       |
      | 7    | 1       | 300       |
    When the batches are appended through the verbatim append path
    Then the offset index names batch ends 3,5,6,7
    And the time index names these running maxima and batch ends
      | timestamp | offset |
      | 102       | 2      |
      | 201       | 5      |
      | 300       | 7      |
    And Kafka dump-log index validation finds no problems

