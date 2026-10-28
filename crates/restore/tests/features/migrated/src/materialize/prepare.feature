Feature: src materialize prepare

  Scenario Outline: tests: owned rewrite checks timestamps by kafka timestamp type
    Then materialize prepare tests: owned rewrite checks timestamps by kafka timestamp type for <example>

    Examples:
      | example         |
      | log append time |
      | create time     |
