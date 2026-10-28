Feature: src features

  Scenario Outline: tests: metadata changed between walks like kafka
    Then features tests: metadata changed between walks like kafka for <example>

    Examples:
      | example                                         |
      | same version                                    |
      | adjacent downgrade 25 to 24                     |
      | adjacent upgrade 24 to 25                       |
      | multi version downgrade without change 29 to 24 |
      | crossing change 25 to 22                        |
      | lower change excluded 25 to 23                  |
      | crossing change 23 to 22                        |
      | crossing change 17 to 15                        |
      | lower change excluded 16 to 15                  |
      | crossing change 13 to 12                        |
      | lower change excluded 12 to 11                  |
      | crossing change 30 to 29                        |
      | crossing change 32 to 31                        |
      | lower change excluded 31 to 30                  |
      | crossing change 8 to 7                          |
      | crossing change 7 to 6                          |
      | crossing change 33 to 32                        |
