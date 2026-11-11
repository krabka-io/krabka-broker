Feature: Freeze scope vocabulary

  Rule: An operator-facing string spells the pattern type in lowercase

    Scenario Outline: Freeze pattern names use Kafka ACL vocabulary
      Given a freeze scope uses Kafka ACL pattern type <pattern>
      When the scope's pattern type is rendered for an operator
      Then the operator-facing pattern type is <name>

      Examples:
        | case     | pattern  | name     |
        | literal  | Literal  | literal  |
        | prefixed | Prefixed | prefixed |

  Rule: A freeze names a concrete pattern type, LITERAL 3 or PREFIXED 4

    Scenario Outline: A pattern type byte names a freeze scope kind only when it is concrete
      Given a freeze request carries pattern type byte <byte>
      When the broker reads the byte as the kind of a freeze scope
      Then the freeze scope kind is <kind>

      Examples:
        | case                  | byte | kind     |
        | literal               | 3    | literal  |
        | prefixed              | 4    | prefixed |
        | unknown               | 0    | none     |
        | any                   | 1    | none     |
        | match                 | 2    | none     |
        | a byte no build knows | 9    | none     |

  Rule: A describe filter of ANY or UNKNOWN reads every entry, and a byte no build knows reads none

    Scenario Outline: A describe filter byte selects the entries of the pattern type it names
      Given a describe request carries pattern type filter byte <byte>
      When the broker reads the byte as a describe filter
      Then the filter selects <selection>

      Examples:
        | case                  | byte | selection        |
        | any                   | 1    | every entry      |
        | unknown               | 0    | every entry      |
        | literal               | 3    | literal entries  |
        | prefixed              | 4    | prefixed entries |
        | a byte no build knows | 9    | no entry         |
