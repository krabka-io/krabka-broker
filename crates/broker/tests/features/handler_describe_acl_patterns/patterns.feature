Feature: DescribeAcls follows Kafka resource pattern matching
  Scenario Outline: Pattern filters distinguish null names exact names and matching prefixes
    Given topic read ACLs for User:alice contain these resource patterns
      | pattern  | name |
      | literal  | foo  |
      | literal  | *    |
      | prefixed | f    |
      | prefixed | fo   |
      | literal  | food |
      | prefixed | bar  |
    And DescribeAcls filters pattern type <pattern> and resource name <name>
    When an admin describes matching ACLs and the result is sorted by resource name
    Then the complete ACL description contains patterns <listed>
    Examples:
      | pattern | name | listed |
      | any | null | literal:*;prefixed:bar;prefixed:f;prefixed:fo;literal:foo;literal:food |
      | any | empty | none |
      | any | foo | literal:foo |
      | match | null | literal:*;prefixed:bar;prefixed:f;prefixed:fo;literal:foo;literal:food |
      | match | empty | literal:* |
      | match | foo | literal:*;prefixed:f;prefixed:fo;literal:foo |
      | literal | null | literal:*;literal:foo;literal:food |
      | literal | empty | none |
      | literal | foo | literal:foo |
      | prefixed | null | prefixed:bar;prefixed:f;prefixed:fo |
      | prefixed | empty | none |
      | prefixed | foo | none |
