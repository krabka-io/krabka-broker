Feature: src format features

  Scenario Outline: tests: feature refusals use kafkas messages
    Then format features tests: feature refusals use kafkas messages for <example>

    Examples:
      | example           |
      | unknown feature   |
      | transaction level |
      | group level       |

  Scenario Outline: tests: feature errors are kafkas formatter errors
    Then format features tests: feature errors are kafkas formatter errors for <example>

    Examples:
      | example                              |
      | unknown feature                      |
      | unknown feature unstable             |
      | group level                          |
      | transaction level                    |
      | negative level                       |
      | metadata below table                 |
      | metadata one below table             |
      | unstable level not stable            |
      | metadata 32 above table              |
      | metadata 33 above table              |
      | metadata 34 above table              |
      | metadata 99 above table              |
      | unstable below table                 |
      | unstable above table                 |
      | unknown release precedes feature     |
      | release and metadata are ambiguous   |
      | ambiguity precedes bad level         |
      | metadata error precedes unknown name |
      | unknown name precedes bad level      |

  Scenario Outline: tests: krabka version bootstraps at level zero unless overridden
    Then format features tests: krabka version bootstraps at level zero unless overridden for <example>

    Examples:
      | example              |
      | no override          |
      | override one         |
      | override zero        |
      | override above range |
