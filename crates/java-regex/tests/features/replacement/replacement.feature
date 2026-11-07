Feature: A Kerberos substitution rewrites a name as Java's replaceFirst and replaceAll do

  The s/pattern/replacement/ part of a sasl.kerberos.principal.to.local.rules
  entry is Java's String.replaceFirst, and with the g flag String.replaceAll.
  The replacement reads $n, ${name} and backslash escapes as
  Matcher.appendReplacement does.

  Rule: Without the g flag only the first match is replaced

    Scenario: A substitution without the g flag replaces the first dot only
      Given a Kerberos substitution s/\./-/
      When the substitution rewrites the name alice.ops.eu
      Then the rewritten name is alice-ops.eu

    Scenario: A substitution with the g flag replaces every dot
      Given a Kerberos substitution s/\./-/g
      When the substitution rewrites the name alice.ops.eu
      Then the rewritten name is alice-ops-eu

  Rule: A group number takes further digits only while they still name a group

    Scenario: A digit after the last group number is literal text
      Given a Kerberos substitution s/([a-z]+)\.ops/$10/
      When the substitution rewrites the name alice.ops
      Then the rewritten name is alice0

    Scenario: An underscore ends the group number
      Given a Kerberos substitution s/([a-z]+)\.ops/$1_x/
      When the substitution rewrites the name alice.ops
      Then the rewritten name is alice_x

  Rule: A named group reference inserts the text of that group

    Scenario: The user group of the principal becomes the name
      Given a Kerberos substitution s/(?<user>[a-z]+)@.*/${user}/
      When the substitution rewrites the name alice@EXAMPLE.COM
      Then the rewritten name is alice

  Rule: A backslash in the replacement makes the next character literal

    Scenario: An escaped dollar sign is not a group reference
      Given a Kerberos substitution s/@.*/\$1/
      When the substitution rewrites the name alice@EXAMPLE.COM
      Then the rewritten name is alice$1

  Rule: A replacement Java refuses fails the rewrite

    Scenario: A group number past the last group is refused
      Given a Kerberos substitution s/([a-z]+)@.*/$2/
      When the substitution tries to rewrite the name alice@EXAMPLE.COM
      Then the rewrite is refused

    Scenario: A group name the pattern does not have is refused
      Given a Kerberos substitution s/([a-z]+)@.*/${team}/
      When the substitution tries to rewrite the name alice@EXAMPLE.COM
      Then the rewrite is refused

    Scenario: A trailing backslash with nothing to escape is refused
      Given a Kerberos substitution s/([a-z]+)@.*/$1\/
      When the substitution tries to rewrite the name alice@EXAMPLE.COM
      Then the rewrite is refused
