Feature: Topic authorization by resource type
  Scenario Outline: Deny ACLs cover only the granted topic names they match
    Given a WRITE allow ACL for topic <grant>
    And a WRITE deny ACL with <pattern> topic pattern <denied_name>
    When Alice requests WRITE authorization by topic resource type
    Then topic resource authorization is <decision>

    Examples:
      | case                                        | grant  | pattern  | denied_name | decision |
      | a literal deny of the granted name          | orders | literal  | orders      | denied   |
      | a prefix the granted name starts with       | orders | prefixed | ord         | denied   |
      | a prefix longer than the granted name       | orders | prefixed | orders-     | allowed  |
      | a literal deny of one name under a wildcard | *      | literal  | hardcode    | allowed  |
      | a prefix deny under a wildcard grant        | *      | prefixed | hard        | allowed  |
      | a literal deny of the wildcard itself       | *      | literal  | *           | denied   |
