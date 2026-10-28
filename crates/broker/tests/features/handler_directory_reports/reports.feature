Feature: AssignReplicasToDirs commits valid reports without disturbing them on invalid subsequent reports
  Scenario: A valid report remains assigned while unknown partitions and UUIDs receive their own errors
    Given broker 1 hosts topic t UUID 187 partition 0 and may report directory UUID 170
    And the broker submits these ordered topic and partition directory reports
      | topic_uuid | partition |
      | 187        | 0         |
      | 187        | 7         |
      | 204        | 0         |
      | 0          | 0         |
    When the active controller handles every directory report in order
    Then the complete directory responses have these ordered partition errors and the valid directory stays assigned
      | topic_uuid | partition | error |
      | 187        | 0         | 0     |
      | 187        | 7         | 3     |
      | 204        | 0         | 100   |
      | 0          | 0         | 100   |
