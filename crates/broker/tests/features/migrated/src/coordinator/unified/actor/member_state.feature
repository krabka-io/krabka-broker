Feature: Kafka compatible subscription expression validation
  Kafka validates a SubscribedTopicRegex with RE2J. The coordinator accepts
  exactly the patterns RE2J accepts, and refuses the others with the message
  Kafka builds from the RE2J syntax error.

  Rule: A refused pattern is answered with Kafka's RE2J syntax error

    Scenario: Unsupported extended mode returns the RE2J client error
      Given a member subscribes with regular expression (?x)abc
      When the coordinator validates the expression using Kafka RE2J syntax rules
      Then the unsupported extended mode flag is rejected with the exact Kafka syntax error

  Rule: The coordinator accepts every pattern RE2J accepts

    Scenario Outline: A pattern RE2J accepts is a valid subscription
      Given a member subscribes with regular expression <pattern>
      When the coordinator validates the expression using Kafka RE2J syntax rules
      Then the expression is accepted

      Examples:
        | case                              | pattern        |
        | a digit class                     | \\d+           |
        | a word class                      | \\w+           |
        | an escaped control character      | \\x08x\\x08    |
        | a Unicode number class            | \\pN           |
        | a Unicode script                  | \\p{Greek}     |
        | a P-style named group             | (?P<name>a)    |
        | an angle-bracket named group      | (?<name>a)     |
        | the case-insensitive flag         | (?i)abc        |
        | two flags                         | (?im)abc       |
        | a cleared flag                    | (?i-s)abc      |
        | the ungreedy flag                 | (?U)a+         |
        | flags scoped to a group           | (?i:abc)       |
        | a non-capturing group             | (?:abc)        |
        | an escaped parenthesis            | \\(?abc        |
        | a parenthesis inside a class      | [(?x]abc       |

  Rule: The coordinator refuses every pattern RE2J refuses

    Scenario Outline: A pattern RE2J refuses is not a valid subscription
      Given a member subscribes with regular expression <pattern>
      When the coordinator validates the expression using Kafka RE2J syntax rules
      Then the expression is refused

      Examples:
        | case                                | pattern    |
        | an unmatched parenthesis            | (          |
        | an unfinished class                 | [a-        |
        | the extended flag                   | (?x) a b c |
        | the extended flag scoped to a group | (?x:abc)   |
        | the Unicode flag                    | (?iu)abc   |
        | a cleared Unicode flag              | (?-u)abc   |
        | a flag RE2J does not have           | (?R)abc    |
        | a flag after an escaped bracket     | a\\[(?x)   |
