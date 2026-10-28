Feature: src controller feature check

  Scenario Outline: tests: a level outside the supported range is refused with kafkas message
    Then controller feature check tests: a level outside the supported range is refused with kafkas message for <example>

    Examples:
      | example                   |
      | latest production         |
      | unstable off              |
      | unstable on               |
      | feature above range       |
      | level below range         |
      | first refusal             |
      | unknown feature           |
      | unknown feature with flag |
      | known beside unknown      |
      | no finalized feature      |
