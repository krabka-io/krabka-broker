Feature: DeleteTopics honours delete.topic.enable

  Rule: With delete.topic.enable false every row is refused and the topic stays

    Scenario Outline: A deletion is refused while topic deletion is disabled
      Given a broker with delete.topic.enable false holds a topic
      When a client deletes the topic by name with DeleteTopics version <version>
      Then the deletion answers <verdict> for the topic
      And the topic still exists

      Examples:
        | case                          | version | verdict                 |
        | before version 3              | 2       | INVALID_REQUEST         |
        | from version 3                | 3       | TOPIC_DELETION_DISABLED |
        | the topics array of version 6 | 6       | TOPIC_DELETION_DISABLED |

  Rule: With delete.topic.enable true the topic is deleted

    Scenario: A deletion succeeds while topic deletion is enabled
      Given a broker with delete.topic.enable true holds a topic
      When a client deletes the topic by name with DeleteTopics version 6
      Then the deletion answers NONE for the topic
      And the topic no longer exists
