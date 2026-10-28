Feature: src simple tests

  Scenario Outline: authorize by resource type: literal and wildcard grants follow kafka pattern dominance
    Then simple tests authorize by resource type: literal and wildcard grants follow kafka pattern dominance for <example>

    Examples:
      | example                     |
      | literal denies same name    |
      | prefix denies within range  |
      | prefix denies outside range |
      | wildcard grant literal deny |
      | wildcard grant prefix deny  |
      | wildcard denies wildcard    |
