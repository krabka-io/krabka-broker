Feature: ConsumerGroupHeartbeat creates a consumer group only for a valid join

  Scenario Outline: Only joins can create consumer groups and other group kinds are refused
    Given the consumer coordinator has a share marker a live share actor and a streams marker
    And consumer member m1 sends epoch <epoch> for group <group>
    When the consumer coordinator handles the member heartbeat
    Then the consumer coordinator returns error <code> message <message> and creates only accepted joins
    And the live share actor is still running

    Examples:
      | case                          | epoch | group               | code | message                                    |
      | heartbeat to an absent group  | 3     | absent-heartbeat    | 69   | Consumer group absent-heartbeat not found. |
      | leave of an absent group      | -1    | absent-leave        | 69   | Group absent-leave not found.              |
      | static leave of absent group  | -2    | absent-static-leave | 69   | Group absent-static-leave not found.       |
      | join of a share marker        | 0     | share               | 69   | Group share is not a consumer group.       |
      | join of a live share actor    | 0     | share-actor         | 69   | Group share-actor is not a consumer group. |
      | heartbeat to a streams group  | 3     | streams             | 69   | Group streams is not a consumer group.     |
      | leave of a streams group      | -1    | streams             | 69   | Group streams is not a consumer group.     |
      | join of a new consumer group  | 0     | joined              | 0    | accepted                                   |
