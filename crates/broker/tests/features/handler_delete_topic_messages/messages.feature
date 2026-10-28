Feature: DeleteTopics response rows carry Kafka default messages
  Scenario Outline: Topic errors explain the failure while success and request-wide failures have no row message
    Given deleting topic t with UUID bytes 3 returns error <code>
    When the DeleteTopics result row is rendered
    Then the topic identity and error remain intact with message <message>
    Examples:
      | code | message                                           |
      | 0    | null                                              |
      | 29   | Topic authorization failed.                       |
      | 3    | This server does not host this topic-partition.   |
      | 100  | This server does not host this topic ID.           |
      | 89   | The throttling quota has been exceeded.           |
      | 41   | null                                              |
