Feature: src topic name

  Scenario Outline: tests: validate topic name matches kafka
    Then topic name tests: validate topic name matches kafka for <example>

    Examples:
      | example                 |
      | ordinary                |
      | allowed punctuation     |
      | max utf16 length        |
      | empty                   |
      | dot                     |
      | dot dot                 |
      | multiple dots           |
      | too long                |
      | slash                   |
      | parent path             |
      | absolute path           |
      | space                   |
      | non ascii               |
      | too long before illegal |
      | astral utf16 too long   |
      | astral utf16 at limit   |

  Scenario Outline: tests: invalid topic name messages are kafka text
    Then topic name tests: invalid topic name messages are kafka text for <example>

    Examples:
      | example           |
      | empty             |
      | dot               |
      | dot dot           |
      | too long          |
      | illegal character |

  Scenario: tests: every byte is legal exactly when kafka allows it
    Then topic name tests: every byte is legal exactly when kafka allows it
