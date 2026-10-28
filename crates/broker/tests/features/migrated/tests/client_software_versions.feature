Feature: tests client software versions

  Scenario Outline: v3 client info validation
    Then client software versions: v3 client info validation for <example>

    Examples:
      | example              |
      | valid                |
      | empty name           |
      | empty version        |
      | invalid name         |
      | leading dash version |

  Scenario: pre v3 does not validate client info
    Then client software versions: pre v3 does not validate client info

  Scenario: accepted v3 handshake bumps client software versions counter
    Then client software versions: accepted v3 handshake bumps client software versions counter

  Scenario: rejected v3 handshake does not bump counter
    Then client software versions: rejected v3 handshake does not bump counter

  Scenario: pre v3 handshake does not bump counter
    Then client software versions: pre v3 handshake does not bump counter
