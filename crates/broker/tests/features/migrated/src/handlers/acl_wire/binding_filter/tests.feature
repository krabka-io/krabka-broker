Feature: Kafka ACL resource pattern queries
  Null and empty resource names differ. MATCH queries include covering prefixes
  and the literal wildcard, while exact pattern queries retain their own scope.

  Background:
    Given these topic ACL resource patterns are stored in order:
      | name | pattern  |
      | foo  | Literal  |
      | *    | Literal  |
      | f    | Prefixed |
      | fo   | Prefixed |
      | bar  | Prefixed |
      | food | Literal  |

  Scenario Outline: Resource filters select exactly Kafka's matching ACL entries
    Given the ACL query uses pattern <pattern> and resource name <name>
    When the broker matches the query against stored ACL resources
    Then the matching resource names are exactly <expected>

    Examples:
      | pattern | name   | expected |
      | Any | null | ["foo","*","f","fo","bar","food"] |
      | Any | "" | [] |
      | Any | "foo" | ["foo"] |
      | Any | "*" | ["*"] |
      | Match | null | ["foo","*","f","fo","bar","food"] |
      | Match | "" | ["*"] |
      | Match | "foo" | ["foo","*","f","fo"] |
      | Match | "food" | ["*","f","fo","food"] |
      | Literal | null | ["foo","*","food"] |
      | Literal | "" | [] |
      | Literal | "foo" | ["foo"] |
      | Prefixed | null | ["f","fo","bar"] |
      | Prefixed | "foo" | [] |
