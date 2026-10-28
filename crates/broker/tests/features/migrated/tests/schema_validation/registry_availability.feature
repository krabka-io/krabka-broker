Feature: tests schema validation registry availability

  Scenario Outline: a registry that cannot answer fails closed
    Then schema validation registry availability: a registry that cannot answer fails closed for <example>

    Examples:
      | example              |
      | no registry section  |
      | registry returns 500 |

  Scenario: fail open admits a record the registry could not answer for
    Then schema validation registry availability: fail open admits a record the registry could not answer for
