Feature: Streams group identity and creation boundaries

  Scenario Outline: Only a join to a free group identity creates a streams group
    Given a join-ready streams broker with share group share and consumer group consumer
    When member m1 sends streams heartbeat epoch <epoch> to group <group>
    Then the streams heartbeat returns code <code>, message "<message>" and streams group existence <exists>

    Examples:
      | group | epoch | code | message | exists |
      | absent-heartbeat | 3 | 69 | Streams group absent-heartbeat not found. | false |
      | absent-leave | -1 | 69 | Group absent-leave not found. | false |
      | share | 0 | 69 | Group share is not a streams group. | false |
      | consumer | 0 | 69 | Group consumer is not a streams group. | false |
      | absent-join | 0 | 0 | null | true |
