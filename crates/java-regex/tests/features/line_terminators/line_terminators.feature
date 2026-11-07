Feature: A dot stops at every Java line terminator

  A KIP-714 CLIENT_METRICS match pattern matches the whole client id. Java's
  line terminators are line feed, carriage return, next line (U+0085), line
  separator (U+2028) and paragraph separator (U+2029). A client id here writes
  an invisible character as its Java escape, such as \n, \r and \u2028. A
  table cell doubles each backslash, as Gherkin requires.

  Rule: A dot does not match any Java line terminator

    Scenario Outline: A dot stops at a line terminator
      Given a client metrics subscription that matches client ids with pattern app-.*
      When the subscription is tested against the client id <id>
      Then the subscription does not select the client

      Examples:
        | case                | id            |
        | line feed           | app-1\\nx     |
        | carriage return     | app-1\\rx     |
        | next line           | app-1\\u0085x |
        | line separator      | app-1\\u2028x |
        | paragraph separator | app-1\\u2029x |

  Rule: (?s) lets a dot match line terminators

    Scenario: A dot under (?s) matches a line separator
      Given a client metrics subscription that matches client ids with pattern (?s)app-.*
      When the subscription is tested against the client id app-1\u2028x
      Then the subscription selects the client

  Rule: (?d) makes line feed the only line terminator

    Scenario Outline: A dot under (?d) matches every other line terminator
      Given a client metrics subscription that matches client ids with pattern (?d)app-.*
      When the subscription is tested against the client id <id>
      Then the subscription selects the client

      Examples:
        | case            | id            |
        | carriage return | app-1\\rx     |
        | line separator  | app-1\\u2028x |

    Scenario: A dot under (?d) does not match a line feed
      Given a client metrics subscription that matches client ids with pattern (?d)app-.*
      When the subscription is tested against the client id app-1\nx
      Then the subscription does not select the client
