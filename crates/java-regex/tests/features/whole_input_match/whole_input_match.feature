Feature: An SSL principal mapping rule matches the whole distinguished name

  Kafka applies an ssl.principal.mapping.rules entry (KIP-371) only when its
  pattern matches the whole distinguished name, as Java's Matcher.matches()
  does. A rule that does not match falls through to the next rule.

  Rule: A pattern matches only when it spans the whole distinguished name

    Scenario: A pattern that spans the distinguished name matches it
      Given an SSL principal mapping rule with pattern CN=([^,]+),OU=ops
      When the rule is tested against the distinguished name CN=alice,OU=ops
      Then the rule matches

    Scenario: A distinguished name with a trailing component does not match
      Given an SSL principal mapping rule with pattern CN=([^,]+),OU=ops
      When the rule is tested against the distinguished name CN=alice,OU=ops,O=acme
      Then the rule does not match

    Scenario: A pattern that spans only the end of the distinguished name does not match
      Given an SSL principal mapping rule with pattern OU=ops
      When the rule is tested against the distinguished name CN=alice,OU=ops
      Then the rule does not match

  Rule: The match backtracks into alternatives and lazy quantifiers until it spans the input

    Scenario: A later alternative spans the name where the first one stops short
      Given an SSL principal mapping rule with pattern CN=(alice|alice-admin)
      When the rule is tested against the distinguished name CN=alice-admin
      Then the rule matches

    Scenario: A lazy quantifier grows until the pattern spans the name
      Given an SSL principal mapping rule with pattern CN=(.*?)
      When the rule is tested against the distinguished name CN=alice
      Then the rule matches

  Rule: A test past the backtrack limit is an error, not a no match

    Java answers such a test, however slowly. Reading the limit as no match
    would let the next rule, or DEFAULT, map the principal to a name the
    operator never meant for it.

    Scenario: A catastrophically backtracking pattern gives no answer
      Given an SSL principal mapping rule with pattern CN=((x+x+)+)(?=y)
      When the rule is tested against the distinguished name CN=xxxxxxxxxxxxxxxxxxxxxxxxx
      Then the test fails at the backtrack limit
