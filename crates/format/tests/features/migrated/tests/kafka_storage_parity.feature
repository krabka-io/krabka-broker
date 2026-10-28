Feature: tests kafka storage parity

  Scenario Outline: the cluster id is written and printed in kafka form
    Then kafka storage parity: the cluster id is written and printed in kafka form for <example>

    Examples:
      | example              |
      | kafka form           |
      | hyphenated           |
      | uppercase hyphenated |
      | padded               |
      | unused bits          |
      | reserved zero        |
      | reserved one         |
      | too short            |
      | too long             |
      | invalid alphabet     |
      | invalid hyphenated   |
      | unhyphenated hex     |

  Scenario Outline: directory ids are written in kafka form
    Then kafka storage parity: directory ids are written in kafka form for <example>

    Examples:
      | example                  |
      | directory id kafka form  |
      | directory id hyphenated  |
      | controller id kafka form |
      | controller id hyphenated |

  Scenario: meta properties is the file kafka writes
    Then kafka storage parity: meta properties is the file kafka writes

  Scenario Outline: every log dir is formatted in one run
    Then kafka storage parity: every log dir is formatted in one run for <example>

    Examples:
      | example         |
      | repeated flags  |
      | comma separated |

  Scenario Outline: a metadata log dir is formatted with the log dirs
    Then kafka storage parity: a metadata log dir is formatted with the log dirs for <example>

    Examples:
      | example                     |
      | separate metadata directory |
      | metadata is log dir         |

  Scenario: ignore formatted skips the formatted and formats the rest
    Then kafka storage parity: ignore formatted skips the formatted and formats the rest

  Scenario Outline: an interrupted run can be run again
    Then kafka storage parity: an interrupted run can be run again for <example>

    Examples:
      | example                       |
      | checkpoint interrupted        |
      | bootstrap records interrupted |
      | manifest interrupted          |
      | temporary marker interrupted  |
      | published marker interrupted  |
