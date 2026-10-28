Feature: CreateAcls validates concrete bindings with Kafka diagnostics
  Scenario: Concrete bindings preserve principal type and resource semantics: resource_code=2, name=topic-a, principal=User:alice (1)
    Given an ACL creation for resource type 2 name topic-a principal User:alice operation 3 permission 3
    When the ACL creation is validated using Kafka 4.3 rules
    Then the stored ACL has resource Topic name topic-a principal User:alice operation Read permission Allow

  Scenario: Concrete bindings preserve principal type and resource semantics: resource_code=2, name=topic-a, principal=Group:ops (2)
    Given an ACL creation for resource type 2 name topic-a principal Group:ops operation 3 permission 3
    When the ACL creation is validated using Kafka 4.3 rules
    Then the stored ACL has resource Topic name topic-a principal Group:ops operation Read permission Allow

  Scenario: Concrete bindings preserve principal type and resource semantics: resource_code=4, name=kafka-cluster, principal=User:alice (3)
    Given an ACL creation for resource type 4 name kafka-cluster principal User:alice operation 7 permission 2
    When the ACL creation is validated using Kafka 4.3 rules
    Then the stored ACL has resource Cluster name kafka-cluster principal User:alice operation Alter permission Deny

  Scenario: Concrete bindings preserve principal type and resource semantics: resource_code=7, name=User:bob, principal=User:alice (4)
    Given an ACL creation for resource type 7 name User:bob principal User:alice operation 13 permission 3
    When the ACL creation is validated using Kafka 4.3 rules
    Then the stored ACL has resource User name User:bob principal User:alice operation CreateTokens permission Allow

  Scenario Outline: Malformed per-binding fields receive Kafka invalid-request messages
    Given the ACL creation field <field> is <value>
    When the ACL creation is validated using Kafka 4.3 rules
    Then ACL creation is refused with code 42 and message <message>
    Examples:
      | field         | value | message                                                                                                                |
      | resource name | empty | Invalid empty resource name                                                                                            |
      | resource type | 99    | Invalid resourceType UNKNOWN                                                                                           |
      | pattern type  | 9     | Invalid patternType UNKNOWN                                                                                            |
      | operation     | 42    | Invalid operation UNKNOWN                                                                                              |
      | permission    | 5     | Invalid permissionType UNKNOWN                                                                                         |
      | principal     | alice | Could not parse principal from `alice` (no colon is present separating the principal type from the principal name)     |
      | host          | empty | Host pattern cannot be null or empty                                                                                   |

  Scenario Outline: The cluster resource has exactly one valid name
    Given the ACL creation field resource type is 4
    And the ACL creation field resource name is <name>
    When the ACL creation is validated using Kafka 4.3 rules
    Then ACL creation is refused with code 42 and message The only valid name for the CLUSTER resource is kafka-cluster
    Examples:
      | name       |
      | my-cluster |
      | empty      |

  Scenario Outline: Whole-request checks distinguish UNKNOWN from filter-only elements
    Given the ACL creation field <field> is <value>
    When the request checks the creation beside a valid baseline for unknown and filter-only elements
    Then unknown elements are <unknown> and filter-only elements are <filter_only>
    Examples:
      | field         | value | unknown | filter_only |
      | resource type | 2     | false   | false       |
      | resource type | 0     | true    | false       |
      | pattern type  | 0     | true    | false       |
      | operation     | 0     | true    | false       |
      | permission    | 0     | true    | false       |
      | resource type | 1     | false   | true        |
      | pattern type  | 1     | false   | true        |
      | pattern type  | 2     | false   | true        |
      | operation     | 1     | false   | true        |
      | permission    | 1     | false   | true        |

  Scenario Outline: Plain and supported CIDR hosts are accepted
    Given the ACL host is <host> and CIDR host support is <supported>
    When the ACL creation is validated using trunk host rules
    Then the ACL host is accepted
    Examples:
      | host          | supported |
      | *             | false     |
      | *             | true      |
      | 10.0.0.1      | false     |
      | 10.0.0.1      | true      |
      | 10.0.0.0/8    | true      |
      | 2001:db8::/32 | true      |

  Scenario Outline: Unsupported malformed and empty hosts preserve precise refusals
    Given the ACL host is <host> and CIDR host support is <supported>
    When the ACL creation is validated using trunk host rules
    Then ACL creation is refused with code <code> and message <message>
    Examples:
      | host          | supported | code | message                                                                        |
      | 10.0.0.0/8    | false     | 35   | CIDR-based ACL host patterns require metadata version 4.4-IV1 or higher.         |
      | 10.0.0.0/33   | true      | 42   | Invalid CIDR notation '10.0.0.0/33': Value [33] not in range [0,32]                |
      | empty         | true      | 42   | Host pattern cannot be null or empty                                            |
      | empty         | false     | 42   | Host pattern cannot be null or empty                                            |
