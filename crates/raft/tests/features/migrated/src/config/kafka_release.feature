Feature: src config kafka release

  Scenario: tests: the table is ascending and inside the vendored ranges
    Then config kafka release tests: the table is ascending and inside the vendored ranges

  Scenario Outline: tests: lookups follow the table
    Then config kafka release tests: lookups follow the table for <example>

    Examples:
      | example             |
      | offset fetch        |
      | sasl handshake      |
      | txn offset commit   |
      | api key 88          |
      | api key 89          |
      | api key 92          |
      | first unknown       |
      | later unknown       |
      | private key unknown |

  Scenario: tests: api table includes produce
    Then config kafka release tests: api table includes produce
