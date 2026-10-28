Feature: Replica directory moves validate destinations, hosting and Cluster Alter authorization

  Scenario: Unknown destination directories are refused
    Given a two-log-directory broker hosts topic t with 1 partitions
    When AlterReplicaLogDirs moves topic t partitions 0 to the unconfigured directory
    Then the move response returns exactly these partition error codes for the requested topic
      | partition | code |
      | 0 | 57 |

  Scenario: Configured destinations cannot move unhosted replicas
    Given a two-log-directory broker hosts no requested topic
    When AlterReplicaLogDirs moves topic missing partitions 0 to the extra directory
    Then the move response returns exactly these partition error codes for the requested topic
      | partition | code |
      | 0 | 9 |

  Scenario: Cluster Alter authorization precedes replica lookup
    Given a two-log-directory broker grants no principal Cluster Alter and hosts no requested topic
    When AlterReplicaLogDirs moves topic t partitions 0,1 to the extra directory
    Then the move response returns exactly these partition error codes for the requested topic
      | partition | code |
      | 0 | 31 |
      | 1 | 31 |
