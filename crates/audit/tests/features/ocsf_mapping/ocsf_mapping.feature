Feature: Audit events are recorded as OCSF 1.3.0

  Each audit event is serialized as one record of the Open Cybersecurity
  Schema Framework, so a SIEM can route it by class without knowing krabka.
  The class, category, activity and type ids are fixed by OCSF, and the type
  id is the class id times 100 plus the activity id. The records are a
  persisted format: the broker keeps writing the krabka 1.0.0 record until the
  cluster finalizes krabka.version 3.

  Rule: Below krabka.version 3 each event keeps the activity krabka 1.0.0 wrote

    Scenario Outline: An event recorded below krabka.version 3 keeps its krabka 1.0.0 activity
      Given the cluster has finalized krabka.version <level>
      When the broker records <event>
      Then the record has OCSF class <class>, category <category> and activity <activity>, with the type id they imply

      Examples:
        | case                                       | level | event                   | class | category | activity |
        | a logon attempt                            | 0     | an authentication       | 3002  | 3        | 1        |
        | a state-changing admin RPC                 | 0     | an admin operation      | 6003  | 6        | 0        |
        | a break-glass transition                   | 0     | a privileged action     | 6003  | 6        | 0        |
        | a broker stop                              | 0     | a broker stop           | 6002  | 6        | 4        |
        | a broker start at the 1.0.0 level          | 1     | a broker start          | 6002  | 6        | 1        |
        | an ACL denial one level below              | 2     | an authorization denial | 3003  | 3        | 2        |
        | a configuration change one level below     | 2     | a configuration change  | 6002  | 6        | 3        |
        | a state-changing admin RPC one level below | 2     | an admin operation      | 6003  | 6        | 0        |

  Rule: From krabka.version 3 each event carries the activity OCSF defines

    Scenario Outline: An event recorded at krabka.version 3 carries its OCSF activity
      Given the cluster has finalized krabka.version 3
      When the broker records <event>
      Then the record has OCSF class <class>, category <category> and activity <activity>, with the type id they imply

      Examples:
        | case                                 | event                   | class | category | activity |
        | a broker start is a Start            | a broker start          | 6002  | 6        | 3        |
        | a broker stop is a Stop              | a broker stop           | 6002  | 6        | 4        |
        | a configuration change is Other      | a configuration change  | 6002  | 6        | 99       |
        | a TLS reload is Other                | a TLS reload            | 6002  | 6        | 99       |
        | an ACL denial is Other               | an authorization denial | 3003  | 3        | 99       |
        | a logon attempt is still a Logon     | an authentication       | 3002  | 3        | 1        |
        | a break-glass transition is Unknown  | a privileged action     | 6003  | 6        | 0        |

    Scenario Outline: An admin operation at krabka.version 3 carries the API activity of its Kafka API
      Given the cluster has finalized krabka.version 3
      When the broker records a successful <api> request
      Then the record has OCSF class 6003, category 6 and activity <activity>, with the type id they imply

      Examples:
        | case                               | api                     | activity |
        | creating topics is a Create        | CreateTopics            | 1        |
        | altering configs is an Update      | IncrementalAlterConfigs | 3        |
        | deleting ACLs is a Delete          | DeleteAcls              | 4        |
        | an API with no such activity       | DescribeCluster         | 0        |

  Rule: From krabka.version 3 a lifecycle record names the broker application

    Scenario: A broker start at krabka.version 3 names the broker application
      Given the cluster has finalized krabka.version 3
      When the broker records a broker start
      Then the record names the application krabka-broker at version 1.0.0

    Scenario: A broker start below krabka.version 3 names no application
      Given the cluster has finalized krabka.version 2
      When the broker records a broker start
      Then the record names no application

  Rule: The OCSF status reports the outcome of the audited operation

    Scenario Outline: The outcome sets the OCSF status
      When the broker records an admin operation that ended in <outcome>
      Then the record has OCSF status <status>

      Examples:
        | case                  | outcome | status |
        | the operation applied | success | 1      |
        | the operation failed  | failure | 2      |

  Rule: A privileged action carries what an auditor needs to re-verify its signature

    Scenario: An applied topic freeze names the signed request
      Given an operator signed a topic freeze before the broker applied it
      When the broker records the applied freeze
      Then the record names the operation topic_freeze.applied
      And the record carries the signature as lowercase hex with the time it was signed

  Rule: Every record declares the OCSF schema version it follows

    Scenario: An audit record declares OCSF schema 1.3.0
      When the broker records an admin operation
      Then the record declares OCSF schema version 1.3.0

  Rule: From krabka.version 3 every record declares that it is a version 2 record

    Scenario Outline: A record at krabka.version 3 declares log version 2
      Given the cluster has finalized krabka.version 3
      When the broker records <event>
      Then the record declares log version 2

      Examples:
        | case                     | event                   |
        | a logon attempt          | an authentication       |
        | an ACL denial            | an authorization denial |
        | a state-changing RPC     | an admin operation      |
        | a break-glass transition | a privileged action     |
        | a broker start           | a broker start          |

    Scenario: A record below krabka.version 3 declares no log version
      Given the cluster has finalized krabka.version 2
      When the broker records an admin operation
      Then the record declares no log version
