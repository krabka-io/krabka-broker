Feature: src kafka hash

  Scenario Outline: tests: murmur2 matches kafka vectors for remainder lengths
    Then kafka hash tests: murmur2 matches kafka vectors for remainder lengths for <example>

    Examples:
      | example     |
      | empty       |
      | one byte    |
      | two bytes   |
      | three bytes |
      | four bytes  |
      | five bytes  |

  Scenario Outline: tests: murmur2 partition matches kafka utils abs mod
    Then kafka hash tests: murmur2 partition matches kafka utils abs mod for <example>

    Examples:
      | example            |
      | transaction id     |
      | producer id        |
      | orders transaction |
      | five byte key      |
