Feature: Kafka topic-name validation in log storage
  Scenario Outline: Legal Kafka topic spellings are accepted
    Given log topic name <name>
    When the log topic name is validated
    Then the log topic name is accepted

    Examples:
      | case                         | name     |
      | a plain name                 | orders   |
      | every legal character class  | a.b_c-D9 |
      | three dots                   | ...      |

  Scenario: The full 249-character topic limit is accepted
    Given a log topic name repeating a 249 times with prefix none
    When the log topic name is validated
    Then the log topic name is accepted

  Scenario: An empty topic name is refused
    Given an empty log topic name
    When the log topic name is validated
    Then the log topic name is refused as empty

  Scenario Outline: Reserved and illegal topic spellings are refused
    Given log topic name <name>
    When the log topic name is validated
    Then the log topic name is refused as <reason>

    Examples:
      | case              | name   | reason            |
      | a single dot      | .      | dot               |
      | two dots          | ..     | dot-dot           |
      | a slash           | a/b    | illegal-character |
      | a relative path   | ../x   | illegal-character |
      | an absolute path  | /tmp/x | illegal-character |
      | a space           | a b    | illegal-character |

  Scenario Outline: UTF-16 length is checked before illegal characters
    Given a log topic name repeating <fragment> <count> times with prefix <prefix>
    When the log topic name is validated
    Then the log topic name is refused as <reason>

    Examples:
      | case                                         | fragment | count | prefix | reason            |
      | one unit over the limit                      | a        | 250   | none   | too-long          |
      | a non-ASCII letter within the limit          | é        | 200   | none   | illegal-character |
      | an illegal character in an overlong name     | a/       | 125   | none   | too-long          |
      | surrogate pairs count as two units           | 😀       | 125   | none   | too-long          |
      | surrogate pairs exactly at the limit         | 😀       | 124   | a      | illegal-character |

  Scenario Outline: Topic-name errors display Kafka diagnostics
    Given topic-name error <kind> for text <name>
    When the topic-name error is displayed
    Then the Kafka topic diagnostic is <message>

    Examples:
      | case                 | kind              | name  | message |
      | an empty name        | empty             | empty | Topic name is invalid: the empty string is not allowed |
      | a single dot         | dot               | .     | Topic name is invalid: '.' is not allowed |
      | two dots             | dot-dot           | ..    | Topic name is invalid: '..' is not allowed |
      | an overlong name     | too-long          | x     | Topic name is invalid: the length of 'x' is longer than the max allowed length 249 |
      | an illegal character | illegal-character | a/b   | Topic name is invalid: 'a/b' contains one or more characters other than ASCII alphanumerics, '.', '_' and '-' |

  Scenario: The topic byte lookup table implements exactly Kafka's character class
    Given all 256 possible topic-name byte values
    When the topic-name byte lookup table classifies them
    Then only ASCII letters digits dot underscore and hyphen are legal topic bytes

  Scenario Outline: Topic names that differ only by dot and underscore collide
    Given a topic named <existing> exists
    When a topic named <requested> is checked for a collision
    Then the topic names collide

    Examples:
      | case                         | existing | requested |
      | a dot against an underscore  | a.b      | a_b       |
      | an underscore against a dot  | a_b      | a.b       |
      | the same name                | a.b      | a.b       |

  Scenario Outline: Topic names that differ otherwise do not collide
    Given a topic named <existing> exists
    When a topic named <requested> is checked for a collision
    Then the topic names do not collide

    Examples:
      | case                         | existing | requested |
      | a hyphen is not a dot        | a.b      | a-b       |
      | a longer name                | a.b      | a.bc      |
