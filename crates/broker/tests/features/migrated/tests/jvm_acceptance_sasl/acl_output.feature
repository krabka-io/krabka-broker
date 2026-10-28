Feature: Reading the ACL bindings reported by Kafka tools
  Scenario: Resource headers qualify every entry in a producer ACL listing
    Given the ACL tool printed
      """
      Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=orders, patternType=LITERAL)`:
        (principal=User:alice, host=*, operation=WRITE, permissionType=ALLOW)
        (principal=User:alice, host=*, operation=DESCRIBE, permissionType=ALLOW)

      Current ACLs for resource `ResourcePattern(resourceType=CLUSTER, name=kafka-cluster, patternType=LITERAL)`:
        (principal=User:alice, host=*, operation=IDEMPOTENT_WRITE, permissionType=ALLOW)
      """
    When the ACL listing is read
    Then every listing contains exactly these bindings
      | resource_type | resource_name | pattern_type | principal  | host | operation        | permission |
      | TOPIC         | orders        | LITERAL      | User:alice | *    | WRITE            | ALLOW      |
      | TOPIC         | orders        | LITERAL      | User:alice | *    | DESCRIBE         | ALLOW      |
      | CLUSTER       | kafka-cluster | LITERAL      | User:alice | *    | IDEMPOTENT_WRITE | ALLOW      |

  Scenario: Resource ordering does not change the ACL set
    Given the ACL tool printed
      """
      Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=orders, patternType=LITERAL)`:
        (principal=User:alice, host=*, operation=WRITE, permissionType=ALLOW)
        (principal=User:alice, host=*, operation=DESCRIBE, permissionType=ALLOW)

      Current ACLs for resource `ResourcePattern(resourceType=CLUSTER, name=kafka-cluster, patternType=LITERAL)`:
        (principal=User:alice, host=*, operation=IDEMPOTENT_WRITE, permissionType=ALLOW)
      """
    When the ACL listing is read in its original and reversed resource order
    Then every listing contains exactly these bindings
      | resource_type | resource_name | pattern_type | principal  | host | operation        | permission |
      | TOPIC         | orders        | LITERAL      | User:alice | *    | WRITE            | ALLOW      |
      | TOPIC         | orders        | LITERAL      | User:alice | *    | DESCRIBE         | ALLOW      |
      | CLUSTER       | kafka-cluster | LITERAL      | User:alice | *    | IDEMPOTENT_WRITE | ALLOW      |

  Scenario: An entry retains its denied host and prefixed resource pattern
    Given the ACL tool printed
      """
      Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=orders, patternType=PREFIXED)`:
        (principal=User:mallory, host=10.0.0.1, operation=READ, permissionType=DENY)
      """
    When the ACL listing is read
    Then every listing contains exactly these bindings
      | resource_type | resource_name | pattern_type | principal    | host     | operation | permission |
      | TOPIC         | orders        | PREFIXED     | User:mallory | 10.0.0.1 | READ      | DENY       |

  Scenario: An entry without a resource header is ignored
    Given the ACL tool printed
      """
        (principal=User:alice, host=*, operation=READ, permissionType=ALLOW)
      """
    When the ACL listing is read
    Then the listing contains no ACL bindings

  Scenario: An empty ACL listing contains no bindings
    Given the ACL tool printed
      """
      """
    When the ACL listing is read
    Then the listing contains no ACL bindings
