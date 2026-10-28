Feature: Kafka ACL authorization

  Scenario: ACL precedence: no acl default denies
    Given ACL facts superuser false allow false deny false default allow false
    When the ACL decision is evaluated
    Then the ACL decision is DenyDefault

  Scenario: ACL precedence: matching allow
    Given ACL facts superuser false allow true deny false default allow false
    When the ACL decision is evaluated
    Then the ACL decision is AllowAcl

  Scenario: ACL precedence: matching deny
    Given ACL facts superuser false allow false deny true default allow false
    When the ACL decision is evaluated
    Then the ACL decision is DenyExplicit

  Scenario: ACL precedence: deny precedes allow
    Given ACL facts superuser false allow true deny true default allow false
    When the ACL decision is evaluated
    Then the ACL decision is DenyExplicit

  Scenario: ACL precedence: superuser bypasses deny
    Given ACL facts superuser true allow false deny true default allow false
    When the ACL decision is evaluated
    Then the ACL decision is AllowSuperuser

  Scenario: ACL precedence: allow default without acl
    Given ACL facts superuser false allow false deny false default allow true
    When the ACL decision is evaluated
    Then the ACL decision is AllowNoAcl

  Scenario: ACL precedence: allow default does not override deny
    Given ACL facts superuser false allow false deny true default allow true
    When the ACL decision is evaluated
    Then the ACL decision is DenyExplicit

  Scenario: ACL precedence: explicit allow is acl grant
    Given ACL facts superuser false allow true deny false default allow true
    When the ACL decision is evaluated
    Then the ACL decision is AllowAcl

  Scenario: Identity wildcards: exact principal
    Given an ACL identity wildcard false exact true
    When the ACL identity is matched
    Then the ACL match is true

  Scenario: Identity wildcards: wildcard principal
    Given an ACL identity wildcard true exact false
    When the ACL identity is matched
    Then the ACL match is true

  Scenario: Identity wildcards: different principal
    Given an ACL identity wildcard false exact false
    When the ACL identity is matched
    Then the ACL match is false

  Scenario: Identity wildcards: wildcard host
    Given an ACL identity wildcard true exact false
    When the ACL identity is matched
    Then the ACL match is true

  Scenario: Resource patterns: literal exact
    Given an ACL resource pattern Literal same type true exact true wildcard false prefix true
    When the ACL resource is matched
    Then the ACL match is true

  Scenario: Resource patterns: literal wildcard
    Given an ACL resource pattern Literal same type true exact false wildcard true prefix false
    When the ACL resource is matched
    Then the ACL match is true

  Scenario: Resource patterns: literal prefix only
    Given an ACL resource pattern Literal same type true exact false wildcard false prefix true
    When the ACL resource is matched
    Then the ACL match is false

  Scenario: Resource patterns: prefixed prefix
    Given an ACL resource pattern Prefixed same type true exact false wildcard false prefix true
    When the ACL resource is matched
    Then the ACL match is true

  Scenario: Resource patterns: prefixed exact
    Given an ACL resource pattern Prefixed same type true exact true wildcard false prefix true
    When the ACL resource is matched
    Then the ACL match is true

  Scenario: Resource patterns: prefixed no match
    Given an ACL resource pattern Prefixed same type true exact false wildcard false prefix false
    When the ACL resource is matched
    Then the ACL match is false

  Scenario: Resource patterns: prefixed wildcard is not special
    Given an ACL resource pattern Prefixed same type true exact false wildcard true prefix false
    When the ACL resource is matched
    Then the ACL match is false

  Scenario: Resource patterns: literal wrong type
    Given an ACL resource pattern Literal same type false exact true wildcard false prefix true
    When the ACL resource is matched
    Then the ACL match is false

  Scenario: Resource patterns: prefixed wrong type
    Given an ACL resource pattern Prefixed same type false exact false wildcard false prefix true
    When the ACL resource is matched
    Then the ACL match is false

  Scenario: Operation implications: same operation allow
    Given an ACL operation Read requested Read allow true
    When the ACL operation is matched
    Then the ACL match is true

  Scenario: Operation implications: same operation deny
    Given an ACL operation Read requested Read allow false
    When the ACL operation is matched
    Then the ACL match is true

  Scenario: Operation implications: all cluster action
    Given an ACL operation All requested ClusterAction allow true
    When the ACL operation is matched
    Then the ACL match is true

  Scenario: Operation implications: all describe deny
    Given an ACL operation All requested Describe allow false
    When the ACL operation is matched
    Then the ACL match is true

  Scenario: Operation implications: read implies describe
    Given an ACL operation Read requested Describe allow true
    When the ACL operation is matched
    Then the ACL match is true

  Scenario: Operation implications: write implies describe
    Given an ACL operation Write requested Describe allow true
    When the ACL operation is matched
    Then the ACL match is true

  Scenario: Operation implications: delete implies describe
    Given an ACL operation Delete requested Describe allow true
    When the ACL operation is matched
    Then the ACL match is true

  Scenario: Operation implications: alter implies describe
    Given an ACL operation Alter requested Describe allow true
    When the ACL operation is matched
    Then the ACL match is true

  Scenario: Operation implications: alter configs implies describe configs
    Given an ACL operation AlterConfigs requested DescribeConfigs allow true
    When the ACL operation is matched
    Then the ACL match is true

  Scenario: Operation implications: deny does not gain describe
    Given an ACL operation Read requested Describe allow false
    When the ACL operation is matched
    Then the ACL match is false

  Scenario: Operation implications: deny does not gain describe configs
    Given an ACL operation AlterConfigs requested DescribeConfigs allow false
    When the ACL operation is matched
    Then the ACL match is false

  Scenario: Operation implications: describe does not imply read
    Given an ACL operation Describe requested Read allow true
    When the ACL operation is matched
    Then the ACL match is false

  Scenario: Operation implications: describe configs does not imply alter configs
    Given an ACL operation DescribeConfigs requested AlterConfigs allow true
    When the ACL operation is matched
    Then the ACL match is false

  Scenario: Operation implications: create does not imply describe
    Given an ACL operation Create requested Describe allow true
    When the ACL operation is matched
    Then the ACL match is false

  Scenario: Operation implications: write does not imply read
    Given an ACL operation Write requested Read allow true
    When the ACL operation is matched
    Then the ACL match is false

  Scenario: Operation implications: alter does not imply alter configs
    Given an ACL operation Alter requested AlterConfigs allow true
    When the ACL operation is matched
    Then the ACL match is false

  Scenario: Authentication admission: pre handshake sasl handshake
    Given authentication state PreHandshake and API key 17
    When request admission is evaluated
    Then the ACL match is true

  Scenario: Authentication admission: pre handshake sasl authenticate
    Given authentication state PreHandshake and API key 18
    When request admission is evaluated
    Then the ACL match is true

  Scenario: Authentication admission: pre handshake token auth
    Given authentication state PreHandshake and API key 36
    When request admission is evaluated
    Then the ACL match is false

  Scenario: Authentication admission: pre handshake negative
    Given authentication state PreHandshake and API key -1
    When request admission is evaluated
    Then the ACL match is false

  Scenario: Authentication admission: pre handshake unknown
    Given authentication state PreHandshake and API key 0
    When request admission is evaluated
    Then the ACL match is false

  Scenario: Authentication admission: exchanging token auth
    Given authentication state Exchanging and API key 36
    When request admission is evaluated
    Then the ACL match is true

  Scenario: Authentication admission: exchanging sasl handshake
    Given authentication state Exchanging and API key 17
    When request admission is evaluated
    Then the ACL match is false

  Scenario: Authentication admission: exchanging sasl authenticate
    Given authentication state Exchanging and API key 18
    When request admission is evaluated
    Then the ACL match is false

  Scenario: Authentication admission: exchanging other
    Given authentication state Exchanging and API key 32767
    When request admission is evaluated
    Then the ACL match is false

  Scenario: Authentication admission: failed token auth
    Given authentication state Failed and API key 36
    When request admission is evaluated
    Then the ACL match is false

  Scenario: Authentication admission: failed sasl handshake
    Given authentication state Failed and API key 17
    When request admission is evaluated
    Then the ACL match is false

  Scenario: Authentication admission: failed sasl authenticate
    Given authentication state Failed and API key 18
    When request admission is evaluated
    Then the ACL match is false

  Scenario: Authentication admission: authenticated zero
    Given authentication state Authenticated and API key 0
    When request admission is evaluated
    Then the ACL match is true

  Scenario: Authentication admission: authenticated maximum
    Given authentication state Authenticated and API key 32767
    When request admission is evaluated
    Then the ACL match is true
