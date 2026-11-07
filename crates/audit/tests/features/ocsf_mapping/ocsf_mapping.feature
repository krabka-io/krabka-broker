Feature: Audit events are recorded as OCSF 1.3.0

  Each audit event is serialized as one record of the Open Cybersecurity
  Schema Framework, so a SIEM can route it by class without knowing krabka.
  The class, category, activity and type ids are fixed by OCSF, and the type
  id is the class id times 100 plus the activity id.

  Rule: Each kind of audit event maps to one OCSF class and activity

    Scenario Outline: An event is recorded under its OCSF class
      When the broker records <event>
      Then the record has OCSF class <class>, category <category>, activity <activity> and type <type>

      Examples:
        | case                       | event                   | class | category | activity | type   |
        | a logon attempt            | an authentication       | 3002  | 3        | 1        | 300201 |
        | an ACL denial              | an authorization denial | 3003  | 3        | 2        | 300302 |
        | a state-changing admin RPC | an admin operation      | 6003  | 6        | 0        | 600300 |
        | a break-glass transition   | a privileged action     | 6003  | 6        | 0        | 600300 |
        | a broker start             | a broker start          | 6002  | 6        | 1        | 600201 |

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
