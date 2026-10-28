Feature: Topic authorization by resource type
  Scenario Outline: Deny ACLs cover only the granted topic names they match
    Given a WRITE allow ACL for topic <grant>
    And a WRITE deny ACL with <pattern> topic pattern <denied_name>
    When Alice requests WRITE authorization by topic resource type
    Then topic resource authorization is <decision>

    Examples:
      | grant  | pattern  | denied_name | decision |
      | orders | literal  | orders      | denied   |
      | orders | prefixed | ord         | denied   |
      | orders | prefixed | orders-     | allowed  |
      | *      | literal  | hardcode    | allowed  |
      | *      | prefixed | hard        | allowed  |
      | *      | literal  | *           | denied   |
