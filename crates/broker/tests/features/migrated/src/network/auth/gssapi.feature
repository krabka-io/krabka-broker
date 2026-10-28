Feature: Authenticated GSSAPI principals follow Kafka short-name rules
  DEFAULT maps service principals only in the configured local realm and
  explicit rules may supply their own realm or case conversion.

  Scenario Outline: Map authenticated principal <principal> to <user>
    Given GSSAPI short-name rules are JSON <rules> with configured realm <realm>
    And SSPI authenticated principal <principal>
    When the authenticated GSSAPI principal is mapped to a local user
    Then the local GSSAPI user is <user>

    Examples:
      | rules | realm | principal | user |
      | ["DEFAULT"] | REALM.COM | alice@realm.com | alice |
      | ["DEFAULT"] | REALM.COM | kafka/host.example.com@realm.com | kafka |
      | ["RULE:[1:$1@$0](.*@OTHER.REALM)s/@.*//"] | unset | alice@other.realm | alice |
      | ["RULE:[1:$1](ali)s/^/x/","RULE:[1:$1]/U"] | unset | alice@realm.com | ALICE |

  Scenario Outline: Refuse authenticated principal <principal> without a matching local rule
    Given GSSAPI short-name rules are JSON ["DEFAULT"] with configured realm <realm>
    And SSPI authenticated principal <principal>
    When the authenticated GSSAPI principal is mapped to a local user
    Then the GSSAPI principal cannot be mapped to a local user

    Examples:
      | realm | principal |
      | REALM.COM | alice@other.realm |
      | unset | alice@other.realm |
      | unset | alice@realm.com |
      | REALM.COM | a/b/c@realm.com |
