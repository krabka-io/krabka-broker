Feature: src coordinator unified streams config

  Scenario: tests: defaults are kafka ga
    Then coordinator unified streams config tests: defaults are kafka ga

  Scenario Outline: tests: rack aware assignment tags parse as kafkas group config does
    Then coordinator unified streams config tests: rack aware assignment tags parse as kafkas group config does for <example>

    Examples:
      | example              |
      | empty                |
      | whitespace           |
      | one tag              |
      | trimmed ordered tags |
      | duplicate tag        |
      | duplicate after trim |
      | empty entry          |
      | trailing empty entry |

  Scenario Outline: tests: share auto offset reset parses the three kafka forms
    Then coordinator unified streams config tests: share auto offset reset parses the three kafka forms for <example>

    Examples:
      | example                 |
      | latest                  |
      | earliest                |
      | one hour                |
      | normalized duration     |
      | zero duration           |
      | negative duration       |
      | negative component      |
      | missing time            |
      | missing date            |
      | empty duration          |
      | missing colon           |
      | missing iso prefix      |
      | invalid component       |
      | weeks                   |
      | out of order components |
      | case sensitive          |
      | unknown                 |
      | empty                   |
