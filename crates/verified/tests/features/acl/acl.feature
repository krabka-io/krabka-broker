Feature: Kafka ACL authorization
  ACL precedence, resource matching and authentication admission are separate rules.
  Each example states the relevant facts and the externally meaningful decision.

  Scenario Outline: Matching deny entries take precedence unless the principal is a superuser
    Given a <principal> principal has matching entries <entries> and the no-ACL policy is <default_policy>
    When the ACL decision is evaluated
    Then authorization is <decision>
    Examples:
      | principal | entries | default_policy | decision |
      | regular | none | deny | denied because no ACL grants access |
      | regular | allow | deny | allowed by a matching ACL |
      | regular | deny | deny | denied by an explicit ACL |
      | regular | allow-and-deny | deny | denied by an explicit ACL |
      | superuser | deny | deny | allowed by superuser status |
      | regular | none | allow | allowed because no ACL applies |
      | regular | deny | allow | denied by an explicit ACL |
      | regular | allow | allow | allowed by a matching ACL |

  Scenario Outline: An exact identity or wildcard makes an ACL applicable
    Given an ACL <identity> entry <stored> is checked against <requested>
    When the ACL identity is matched
    Then the ACL applies: <applies>
    Examples:
      | identity | stored | requested | applies |
      | principal | User:alice | User:alice | true |
      | principal | * | User:alice | true |
      | principal | User:bob | User:alice | false |
      | host | * | 127.0.0.1 | true |

  Scenario: Resource type and literal or prefixed names decide ACL applicability (1: case literal exact, pattern Literal)
    Given a Literal ACL on topic named orders is checked against topic named orders
    When the ACL resource is matched
    Then the ACL applies: true

  Scenario: Resource type and literal or prefixed names decide ACL applicability (2: case literal wildcard, pattern Literal)
    Given a Literal ACL on topic named * is checked against topic named orders
    When the ACL resource is matched
    Then the ACL applies: true

  Scenario: Resource type and literal or prefixed names decide ACL applicability (3: case literal prefix only, pattern Literal)
    Given a Literal ACL on topic named orders is checked against topic named orders-2026
    When the ACL resource is matched
    Then the ACL applies: false

  Scenario: Resource type and literal or prefixed names decide ACL applicability (4: case prefixed prefix, pattern Prefixed)
    Given a Prefixed ACL on topic named orders is checked against topic named orders-2026
    When the ACL resource is matched
    Then the ACL applies: true

  Scenario: Resource type and literal or prefixed names decide ACL applicability (5: case prefixed exact, pattern Prefixed)
    Given a Prefixed ACL on topic named orders is checked against topic named orders
    When the ACL resource is matched
    Then the ACL applies: true

  Scenario: Resource type and literal or prefixed names decide ACL applicability (6: case prefixed no match, pattern Prefixed)
    Given a Prefixed ACL on topic named payments is checked against topic named orders
    When the ACL resource is matched
    Then the ACL applies: false

  Scenario: Resource type and literal or prefixed names decide ACL applicability (7: case prefixed wildcard is not special, pattern Prefixed)
    Given a Prefixed ACL on topic named * is checked against topic named orders
    When the ACL resource is matched
    Then the ACL applies: false

  Scenario: Resource type and literal or prefixed names decide ACL applicability (8: case literal wrong type, pattern Literal)
    Given a Literal ACL on topic named orders is checked against group named orders
    When the ACL resource is matched
    Then the ACL applies: false

  Scenario: Resource type and literal or prefixed names decide ACL applicability (9: case prefixed wrong type, pattern Prefixed)
    Given a Prefixed ACL on topic named orders is checked against group named orders-2026
    When the ACL resource is matched
    Then the ACL applies: false

  Scenario Outline: Allow ACLs imply selected describe operations but deny ACLs do not
    Given an ACL <permission> entry for <stored> is checked against requested operation <requested>
    When the ACL operation is matched
    Then the ACL applies: <applies>

    Examples:
      | case | stored | requested | permission | applies |
      | same operation allow | Read | Read | Allow | true |
      | same operation deny | Read | Read | Deny | true |
      | all cluster action | All | ClusterAction | Allow | true |
      | all describe deny | All | Describe | Deny | true |
      | read implies describe | Read | Describe | Allow | true |
      | write implies describe | Write | Describe | Allow | true |
      | delete implies describe | Delete | Describe | Allow | true |
      | alter implies describe | Alter | Describe | Allow | true |
      | alter configs implies describe configs | AlterConfigs | DescribeConfigs | Allow | true |
      | deny does not gain describe | Read | Describe | Deny | false |
      | deny does not gain describe configs | AlterConfigs | DescribeConfigs | Deny | false |
      | describe does not imply read | Describe | Read | Allow | false |
      | describe configs does not imply alter configs | DescribeConfigs | AlterConfigs | Allow | false |
      | create does not imply describe | Create | Describe | Allow | false |
      | write does not imply read | Write | Read | Allow | false |
      | alter does not imply alter configs | Alter | AlterConfigs | Allow | false |

  Scenario Outline: Only the authentication handshake APIs are admitted before authentication completes
    Given authentication state <state>
    When a <request> API request is admitted
    Then the authentication state admits the request: <admitted>

    Examples:
      | case | state | request | admitted |
      | pre handshake sasl handshake | PreHandshake | SaslHandshake | true |
      | pre handshake ApiVersions | PreHandshake | ApiVersions | true |
      | pre handshake SaslAuthenticate | PreHandshake | SaslAuthenticate | false |
      | pre handshake negative | PreHandshake | Invalid | false |
      | pre handshake unknown | PreHandshake | Produce | false |
      | exchanging SaslAuthenticate | Exchanging | SaslAuthenticate | true |
      | exchanging sasl handshake | Exchanging | SaslHandshake | false |
      | exchanging ApiVersions | Exchanging | ApiVersions | false |
      | exchanging other | Exchanging | Unsupported | false |
      | failed SaslAuthenticate | Failed | SaslAuthenticate | false |
      | failed sasl handshake | Failed | SaslHandshake | false |
      | failed ApiVersions | Failed | ApiVersions | false |
      | authenticated zero | Authenticated | Produce | true |
      | authenticated maximum | Authenticated | Unsupported | true |
