Feature: A client metrics match pattern compiles as Java's Pattern.compile does

  An operator sets the match patterns of a KIP-714 CLIENT_METRICS
  subscription. A pattern Java's Pattern.compile refuses makes the alteration
  fail with INVALID_CONFIG. A table cell doubles each backslash of a pattern, as
  Gherkin requires.

  Rule: A pattern Java refuses is refused

    Scenario Outline: A pattern Java refuses is refused
      When an operator writes the client metrics match pattern <pattern>
      Then the pattern is refused as invalid

      Examples:
        | case                         | pattern        |
        | duplicate group name         | (?<n>a)(?<n>b) |
        | dangling quantifier          | *app           |
        | backwards repeat range       | app{3,1}       |
        | repeat without a least count | app{,3}        |
        | unknown property             | \\p{Foo}       |
        | unclosed group               | (app           |

  Rule: Syntax that only Java has keeps its Java meaning

    Scenario Outline: Java-only syntax leaves out a client that other engines select
      Given a client metrics subscription that matches client ids with pattern <pattern>
      When the subscription is tested against the client id <id>
      Then the subscription does not select the client

      Examples:
        | case                                       | pattern           | id     |
        | a possessive quantifier gives nothing back | app-\\d*+1        | app-11 |
        | an atomic group does not backtrack         | (?>app\|app-1)    | app-1  |
        | a quoted dot is not a wildcard             | \\Qapp.1\\E       | appx1  |

  Rule: A pattern Java compiles that cannot be read as Java reads it is refused

    Refusing it keeps a subscription from selecting clients that Java would
    not select. The crate documents each such pattern under Limits.

    Scenario Outline: A pattern beyond what can be read as Java reads it is refused
      When an operator writes the client metrics match pattern <pattern>
      Then the pattern is refused as invalid

      Examples:
        | case                             | pattern      |
        | Unicode block                    | \\p{InGreek} |
        | quantified look-ahead            | (?=a)*a      |
        | backreference to a missing group | (a)\\2       |
