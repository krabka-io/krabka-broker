Feature: Topic authorization by resource type

  Rule: Topic WRITE authorization requires a grant
    Scenario: Alice has no topic grants
      When Alice requests WRITE authorization by topic resource type
      Then topic resource authorization is denied

    Scenario: One topic grant is sufficient
      Given a WRITE allow ACL with literal topic pattern orders
      When Alice requests WRITE authorization by topic resource type
      Then topic resource authorization is allowed

  Rule: A denial defeats a literal grant only when it covers the granted name
    Scenario Outline: A literal grant survives unrelated denials
      Given a WRITE allow ACL with literal topic pattern orders
      And a WRITE deny ACL with <pattern> topic pattern <denied_name>
      When Alice requests WRITE authorization by topic resource type
      Then topic resource authorization is <decision>

      Examples:
        | case              | pattern  | denied_name | decision |
        | exact name        | literal  | orders      | denied   |
        | different name    | literal  | invoices    | allowed  |
        | containing prefix | prefixed | ord         | denied   |
        | longer prefix     | prefixed | orders-     | allowed  |
        | every topic       | literal  | *           | denied   |

  Rule: A denial defeats a prefix grant only when it covers the whole prefix
    Scenario Outline: A prefix grant survives exclusions of particular topics
      Given a WRITE allow ACL with prefixed topic pattern orders-
      And a WRITE deny ACL with <pattern> topic pattern <denied_name>
      When Alice requests WRITE authorization by topic resource type
      Then topic resource authorization is <decision>

      Examples:
        | case                    | pattern  | denied_name     | decision |
        | equal prefix            | prefixed | orders-         | denied   |
        | containing prefix       | prefixed | ord             | denied   |
        | narrower prefix         | prefixed | orders-private- | allowed  |
        | exact prefix name       | literal  | orders-         | allowed  |
        | one topic in the prefix | literal  | orders-private  | allowed  |
        | unrelated prefix        | prefixed | invoices-       | allowed  |
        | every topic             | literal  | *               | denied   |

  Rule: A wildcard grant is defeated only by a wildcard denial
    Scenario Outline: A wildcard grant survives exclusions of particular names
      Given a WRITE allow ACL with literal topic pattern *
      And a WRITE deny ACL with <pattern> topic pattern <denied_name>
      When Alice requests WRITE authorization by topic resource type
      Then topic resource authorization is <decision>

      Examples:
        | case                       | pattern  | denied_name | decision |
        | Kafka's initial probe name | literal  | hardcode    | allowed  |
        | prefix of that probe name  | prefixed | hard        | allowed  |
        | every topic                | literal  | *           | denied   |
