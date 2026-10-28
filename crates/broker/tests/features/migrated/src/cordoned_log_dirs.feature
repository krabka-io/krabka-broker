Feature: src cordoned log dirs

  Scenario Outline: tests: values resolve as kafka validates them
    Then cordoned log dirs tests: values resolve as kafka validates them for <example>

    Examples:
      | example                      |
      | empty                        |
      | whitespace                   |
      | one directory                |
      | trimmed ordered directories  |
      | duplicate directory          |
      | wildcard                     |
      | trailing empty entry         |
      | wildcard with directory      |
      | missing directory            |
      | multiple missing directories |
