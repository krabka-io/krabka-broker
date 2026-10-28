Feature: Controller topic creation failures retain Kafka operator messages

  Scenario: A controller request timeout reports the failed operation
    Given the controller CreateTopics request timed out
    When the broker renders the controller topic creation failure
    Then the topic creation failure message is CreateTopicsRequest to controller timed out

  Scenario: A refused controller envelope reports Kafka authorization text
    Given the controller refused the forwarded CreateTopics envelope with code 31
    When the broker renders the controller topic creation failure
    Then the topic creation failure message is Cluster authorization failed.

  Scenario: An envelope error without specialized text preserves its numeric code
    Given the controller refused the forwarded CreateTopics envelope with code 97
    When the broker renders the controller topic creation failure
    Then the topic creation failure message is the controller refused the envelope with error code 97

  Scenario: A protocol codec failure preserves the exact underlying diagnostic
    Given the controller topic codec failed with codec: bad
    When the broker renders the controller topic creation failure
    Then the topic creation failure message is codec: bad
