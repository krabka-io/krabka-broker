Feature: Character classes and case folding are ASCII unless a pattern asks for Unicode

  A KIP-714 CLIENT_METRICS subscription selects a client when a match pattern
  matches the whole client id, as Java's Matcher.matches() does. Java reads
  \w, \d and (?i) as ASCII only, where many other regex engines read them as
  Unicode.

  Rule: \w and \d match ASCII characters only by default

    Scenario: \w does not match an accented letter
      Given a client metrics subscription that matches client ids with pattern \w+
      When the subscription is tested against the client id jörg
      Then the subscription does not select the client

    Scenario: \d does not match an Arabic-Indic digit
      Given a client metrics subscription that matches client ids with pattern app-\d+
      When the subscription is tested against the client id app-٣
      Then the subscription does not select the client

  Rule: (?U) makes \w and \d match Unicode characters

    Scenario: \w under (?U) matches an accented letter
      Given a client metrics subscription that matches client ids with pattern (?U)\w+
      When the subscription is tested against the client id jörg
      Then the subscription selects the client

    Scenario: \d under (?U) matches an Arabic-Indic digit
      Given a client metrics subscription that matches client ids with pattern (?U)app-\d+
      When the subscription is tested against the client id app-٣
      Then the subscription selects the client

  Rule: (?i) folds ASCII case only, and (?iu) folds Unicode case too

    Scenario: (?i) matches an ASCII letter of the other case
      Given a client metrics subscription that matches client ids with pattern (?i)service-.*
      When the subscription is tested against the client id SERVICE-a
      Then the subscription selects the client

    Scenario: (?i) does not fold the long s to s
      Given a client metrics subscription that matches client ids with pattern (?i)service-.*
      When the subscription is tested against the client id ſervice-a
      Then the subscription does not select the client

    Scenario: (?iu) folds the long s to s
      Given a client metrics subscription that matches client ids with pattern (?iu)service-.*
      When the subscription is tested against the client id ſervice-a
      Then the subscription selects the client
