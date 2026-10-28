Feature: Kafka topic-name validation in log storage
  Scenario Outline: Legal Kafka topic spellings are accepted
    Given log topic name <name>
    When the log topic name is validated
    Then the log topic name is accepted

    Examples:
      | name     |
      | orders   |
      | a.b_c-D9 |
      | ...      |

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
      | name   | reason            |
      | .      | dot               |
      | ..     | dot-dot           |
      | a/b    | illegal-character |
      | ../x   | illegal-character |
      | /tmp/x | illegal-character |
      | a b    | illegal-character |

  Scenario Outline: UTF-16 length is checked before illegal characters
    Given a log topic name repeating <fragment> <count> times with prefix <prefix>
    When the log topic name is validated
    Then the log topic name is refused as <reason>

    Examples:
      | fragment | count | prefix | reason            |
      | a        | 250   | none   | too-long          |
      | é        | 200   | none   | illegal-character |
      | a/       | 125   | none   | too-long          |
      | 😀       | 125   | none   | too-long          |
      | 😀       | 124   | a      | illegal-character |

  Scenario Outline: Topic-name errors display Kafka diagnostics
    Given topic-name error <kind> for text <name>
    When the topic-name error is displayed
    Then the Kafka topic diagnostic is <message>

    Examples:
      | kind              | name  | message |
      | empty             | empty | Topic name is invalid: the empty string is not allowed |
      | dot               | .     | Topic name is invalid: '.' is not allowed |
      | dot-dot           | ..    | Topic name is invalid: '..' is not allowed |
      | too-long          | x     | Topic name is invalid: the length of 'x' is longer than the max allowed length 249 |
      | illegal-character | a/b   | Topic name is invalid: 'a/b' contains one or more characters other than ASCII alphanumerics, '.', '_' and '-' |

  Scenario: The topic byte lookup table implements exactly Kafka's character class
    Given all 256 possible topic-name byte values
    When the topic-name byte lookup table classifies them
    Then only ASCII letters digits dot underscore and hyphen are legal topic bytes
