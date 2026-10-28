Feature: Controller mutation quota admits a first strict mutation and accounts for throttling
  Scenario: controller mutation rate throttles create topics
    Given alice has Cluster Create permission and controller mutation rate 2
    When alice creates topic throttled-topic with 10 partitions
    Then the topic mutation has error code 0
    And the topic mutation reports no throttle delay
    When alice creates topic rejected-topic with 1 partitions
    Then the topic mutation has error code 89
    And the topic mutation reports a positive throttle delay

  Scenario: unthrottled create topics unaffected
    Given admin is a super user without a controller mutation quota
    When admin creates topic unthrottled-topic with 10 partitions
    Then the topic mutation has error code 0
    And the topic mutation reports no throttle delay

  Scenario: controller mutation rate throttles delete topics
    Given alice may Delete a ten partition topic and has controller mutation rate 2
    When alice deletes to-delete
    Then the topic mutation has error code 0
    And the topic mutation reports a positive throttle delay
    And quota and DeleteTopics latency metrics both cover the reported applied throttle
