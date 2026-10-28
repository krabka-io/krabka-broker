Feature: src args value

  Scenario: tests: parse cluster id accepts kafka base64 and hyphenated uuid forms
    Then args value tests: parse cluster id accepts kafka base64 and hyphenated uuid forms

  Scenario Outline: tests: topic names accept kafka names
    Then args value tests: topic names accept kafka names for <example>

    Examples:
      | example        |
      | common         |
      | one character  |
      | punctuation    |
      | digit          |
      | maximum length |

  Scenario Outline: tests: topic names reject invalid kafka names
    Then args value tests: topic names reject invalid kafka names for <example>

    Examples:
      | example    |
      | empty      |
      | dot        |
      | double dot |
      | whitespace |
      | slash      |
      | colon      |
      | equals     |
      | non ascii  |
      | over limit |

  Scenario: tests: partition ref displays the kafka spelling
    Then args value tests: partition ref displays the kafka spelling

  Scenario Outline: tests: node ids reject values outside kafkas node id range
    Then args value tests: node ids reject values outside kafkas node id range for <example>

    Examples:
      | example  |
      | negative |
      | text     |
      | empty    |
      | overflow |

  Scenario: tests: node ids accept kafkas node id range
    Then args value tests: node ids accept kafkas node id range
