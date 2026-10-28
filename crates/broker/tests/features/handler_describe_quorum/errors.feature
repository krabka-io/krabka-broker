Feature: DescribeQuorum supplies Kafka explanations for whole-request errors
  Scenario Outline: Top-level refusals contain the standard error message
    Given DescribeQuorum refuses the whole request with error <code>
    When the top-level quorum error response is built
    Then the quorum response carries the requested error and Kafka message <message>
    Examples:
      | code | message                                                                                                                                                                                                                                            |
      | 31   | Cluster authorization failed.                                                                                                                                                                                                                      |
      | 6    | For requests intended only for the leader, this error indicates that the broker is not the current leader. For requests intended for any replica, this error indicates that the broker is not a replica of the topic partition.                    |
