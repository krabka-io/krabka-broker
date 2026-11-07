Feature: A step placeholder parses into a StepParam enum

  Scenario Outline: A cleanup policy placeholder parses and writes back its primary word
    Given a log has cleanup policy <policy>
    When the cleanup policy is written back as step text
    Then the step text is <text>

    Examples:
      | case               | policy         | text           |
      | delete only        | delete         | delete         |
      | compact and delete | compact,delete | compact,delete |
      | no cleanup         | none           | none           |
      | no cleanup alias   | off            | none           |
      | default kebab case | log-compaction | log-compaction |
