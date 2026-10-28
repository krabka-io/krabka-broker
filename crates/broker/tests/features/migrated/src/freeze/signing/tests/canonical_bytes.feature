Feature: Freeze signing pattern-type separation
  A literal topic and a prefixed scope must sign different Kafka pattern bytes,
  so one scope's operator signature can never authorize the other scope.

  Scenario Outline: Signed freeze records preserve Kafka pattern-type bytes
    Given a signed freeze record uses pattern type <pattern>
    When the canonical freeze signing payload is encoded
    Then the signed payload's pattern-type byte is <byte>

    Examples:
      | pattern  | byte |
      | Literal  | 3    |
      | Prefixed | 4    |
