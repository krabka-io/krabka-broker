Feature: Kafka Kerberos principal-to-local-name rules
  ACL short names follow Kafka's own auth_to_local grammar: default realm,
  whole-name matches, component parameters, first matching rule, substitutions,
  case conversion, and refusal of non-simple mapped names.

  Scenario: DEFAULT maps a one-component principal in the default realm
    Given the Kerberos default realm is REALM.COM
    And the ordered Kerberos short-name rules are:
      | rule |
      | DEFAULT |
    And the authenticated Kerberos principal is user@REALM.COM
    When the broker derives the Kerberos principal's ACL short name
    Then the Kerberos ACL short name is user

  Scenario: DEFAULT maps the first component of a two-component principal
    Given the Kerberos default realm is REALM.COM
    And the ordered Kerberos short-name rules are:
      | rule |
      | DEFAULT |
    And the authenticated Kerberos principal is user/host@REALM.COM
    When the broker derives the Kerberos principal's ACL short name
    Then the Kerberos ACL short name is user

  Scenario: DEFAULT maps a Kafka service principal
    Given the Kerberos default realm is REALM.COM
    And the ordered Kerberos short-name rules are:
      | rule |
      | DEFAULT |
    And the authenticated Kerberos principal is kafka/host.example.com@REALM.COM
    When the broker derives the Kerberos principal's ACL short name
    Then the Kerberos ACL short name is kafka

  Scenario: DEFAULT refuses a one-component principal from another realm
    Given the Kerberos default realm is REALM.COM
    And the ordered Kerberos short-name rules are:
      | rule |
      | DEFAULT |
    And the authenticated Kerberos principal is user@OTHER.REALM
    When the broker derives the Kerberos principal's ACL short name
    Then the Kerberos principal cannot be mapped to an ACL short name

  Scenario: DEFAULT refuses a two-component principal from another realm
    Given the Kerberos default realm is REALM.COM
    And the ordered Kerberos short-name rules are:
      | rule |
      | DEFAULT |
    And the authenticated Kerberos principal is user/host@OTHER.REALM
    When the broker derives the Kerberos principal's ACL short name
    Then the Kerberos principal cannot be mapped to an ACL short name

  Scenario: An already-short principal survives a configured mapping rule
    Given the Kerberos default realm is REALM.COM
    And the ordered Kerberos short-name rules are:
      | rule |
      | RULE:[1:$1]s/x/y/ |
    And the authenticated Kerberos principal is user
    When the broker derives the Kerberos principal's ACL short name
    Then the Kerberos ACL short name is user

  Scenario: An already-short principal needs no mapping rules
    Given the Kerberos default realm is REALM.COM
    And the ordered Kerberos short-name rules are:
      | rule |
    And the authenticated Kerberos principal is user
    When the broker derives the Kerberos principal's ACL short name
    Then the Kerberos ACL short name is user

  Scenario: A partial match falls through to DEFAULT rather than rewriting a prefix
    Given the Kerberos default realm is REALM.COM
    And the ordered Kerberos short-name rules are:
      | rule |
      | RULE:[1:$1](ali)s/^/x/ |
      | DEFAULT |
    And the authenticated Kerberos principal is alice@REALM.COM
    When the broker derives the Kerberos principal's ACL short name
    Then the Kerberos ACL short name is alice

  Scenario: A whole-name wildcard match permits prefix substitution
    Given the Kerberos default realm is REALM.COM
    And the ordered Kerberos short-name rules are:
      | rule |
      | RULE:[1:$1](ali.*)s/^/x/ |
    And the authenticated Kerberos principal is alice@REALM.COM
    When the broker derives the Kerberos principal's ACL short name
    Then the Kerberos ACL short name is xalice

  Scenario: An alternative pattern backtracks to match the entire name
    Given the Kerberos default realm is REALM.COM
    And the ordered Kerberos short-name rules are:
      | rule |
      | RULE:[1:$1](a\|al)s/^/x/ |
    And the authenticated Kerberos principal is al@REALM.COM
    When the broker derives the Kerberos principal's ACL short name
    Then the Kerberos ACL short name is xal

  Scenario: The lower-case flag normalizes a mapped principal
    Given the Kerberos default realm is REALM.COM
    And the ordered Kerberos short-name rules are:
      | rule |
      | RULE:[1:$1]/L |
    And the authenticated Kerberos principal is Alice@REALM.COM
    When the broker derives the Kerberos principal's ACL short name
    Then the Kerberos ACL short name is alice

  Scenario: The upper-case flag normalizes a mapped principal
    Given the Kerberos default realm is REALM.COM
    And the ordered Kerberos short-name rules are:
      | rule |
      | RULE:[1:$1]/U |
    And the authenticated Kerberos principal is Alice@REALM.COM
    When the broker derives the Kerberos principal's ACL short name
    Then the Kerberos ACL short name is ALICE

  Scenario: Global substitution precedes upper-case conversion
    Given the Kerberos default realm is REALM.COM
    And the ordered Kerberos short-name rules are:
      | rule |
      | RULE:[1:$1](.*)s/A/a/g/U |
    And the authenticated Kerberos principal is BANANA@REALM.COM
    When the broker derives the Kerberos principal's ACL short name
    Then the Kerberos ACL short name is BANANA

  Scenario: First-match substitution precedes lower-case conversion
    Given the Kerberos default realm is REALM.COM
    And the ordered Kerberos short-name rules are:
      | rule |
      | RULE:[1:$1]s/A/a/L |
    And the authenticated Kerberos principal is Alice@REALM.COM
    When the broker derives the Kerberos principal's ACL short name
    Then the Kerberos ACL short name is alice

  Scenario: The realm is available as rule format parameter zero
    Given the Kerberos default realm is REALM.COM
    And the ordered Kerberos short-name rules are:
      | rule |
      | RULE:[2:$1@$0](.*@REALM.COM)s/@.*// |
    And the authenticated Kerberos principal is user/host@REALM.COM
    When the broker derives the Kerberos principal's ACL short name
    Then the Kerberos ACL short name is user

  Scenario: The host component can be reordered with the service component
    Given the Kerberos default realm is REALM.COM
    And the ordered Kerberos short-name rules are:
      | rule |
      | RULE:[2:$1.$2@$0]s/^(.*)\\.(.*)@.*$/$2-$1/ |
    And the authenticated Kerberos principal is user/host@REALM.COM
    When the broker derives the Kerberos principal's ACL short name
    Then the Kerberos ACL short name is host-user

  Scenario: A mismatched component-count rule falls through to the next rule
    Given the Kerberos default realm is REALM.COM
    And the ordered Kerberos short-name rules are:
      | rule |
      | RULE:[2:$1]s/^/two-/ |
      | RULE:[1:$1]s/^/one-/ |
    And the authenticated Kerberos principal is alice@REALM.COM
    When the broker derives the Kerberos principal's ACL short name
    Then the Kerberos ACL short name is one-alice

  Scenario: A mapped slash is an error rather than a DEFAULT fallback
    Given the Kerberos default realm is REALM.COM
    And the ordered Kerberos short-name rules are:
      | rule |
      | RULE:[2:$1/$2] |
      | DEFAULT |
    And the authenticated Kerberos principal is user/host@REALM.COM
    When the broker derives the Kerberos principal's ACL short name
    Then the Kerberos principal cannot be mapped to an ACL short name

  Scenario: A mapped realm separator is an error rather than a DEFAULT fallback
    Given the Kerberos default realm is REALM.COM
    And the ordered Kerberos short-name rules are:
      | rule |
      | RULE:[1:$1@$0] |
      | DEFAULT |
    And the authenticated Kerberos principal is user@REALM.COM
    When the broker derives the Kerberos principal's ACL short name
    Then the Kerberos principal cannot be mapped to an ACL short name

  Scenario: A principal with no matching rule is refused
    Given the Kerberos default realm is REALM.COM
    And the ordered Kerberos short-name rules are:
      | rule |
      | RULE:[1:$1](nope)s/a/b/ |
    And the authenticated Kerberos principal is alice@REALM.COM
    When the broker derives the Kerberos principal's ACL short name
    Then the Kerberos principal cannot be mapped to an ACL short name

  Scenario: A principal with three components is malformed
    Given the Kerberos default realm is REALM.COM
    And the ordered Kerberos short-name rules are:
      | rule |
      | DEFAULT |
    And the authenticated Kerberos principal is a/b/c@REALM.COM
    When the broker derives the Kerberos principal's ACL short name
    Then the Kerberos principal cannot be mapped to an ACL short name

  Scenario: A nonnumeric format parameter prevents short-name mapping
    Given the Kerberos default realm is REALM.COM
    And the ordered Kerberos short-name rules are:
      | rule |
      | RULE:[1:$x] |
    And the authenticated Kerberos principal is alice@REALM.COM
    When the broker derives the Kerberos principal's ACL short name
    Then the Kerberos principal cannot be mapped to an ACL short name
