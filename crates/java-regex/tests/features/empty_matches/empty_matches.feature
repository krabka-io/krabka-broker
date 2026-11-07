Feature: A Kerberos substitution finds empty matches as Java's Matcher.find does

  A global substitution, s/pattern/replacement/g, replaces each match that
  Java's Matcher.find gives, empty matches included. Replacement syntax is in
  ../replacement/replacement.feature.

  Rule: After a match that is not empty, an empty match can follow where it ended

    Scenario: A global substitution of the whole name appends twice
      Given a Kerberos substitution s/(.*)/$1x/g
      When the substitution rewrites the name alice
      Then the rewritten name is alicexx

    Scenario: A substitution without the g flag of the whole name appends once
      Given a Kerberos substitution s/(.*)/$1x/
      When the substitution rewrites the name alice
      Then the rewritten name is alicex

  Rule: After an empty match the search moves on one character

    Scenario: A pattern that only matches empty text marks every gap
      Given a Kerberos substitution s/x*/-/g
      When the substitution rewrites the name abc
      Then the rewritten name is -a-b-c-

  Rule: \G anchors each match where the previous match ended

    Scenario: A \G substitution stops at the first character that breaks the run
      Given a Kerberos substitution s/\Ga/b/g
      When the substitution rewrites the name aaxa
      Then the rewritten name is bbxa

    Scenario: Without \G every match is replaced
      Given a Kerberos substitution s/a/b/g
      When the substitution rewrites the name aaxa
      Then the rewritten name is bbxb
