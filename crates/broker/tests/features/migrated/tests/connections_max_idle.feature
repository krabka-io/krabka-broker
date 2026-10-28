Feature: tests connections max idle

  Scenario Outline: silent plaintext connection closes past its idle window
    Then connections max idle: silent plaintext connection closes past its idle window for <example>

    Examples:
      | example           |
      | broker wide       |
      | listener override |

  Scenario: a connection speaking each half window is never closed
    Then connections max idle: a connection speaking each half window is never closed

  Scenario: a request trickled across two windows is served
    Then connections max idle: a request trickled across two windows is served

  Scenario: a tls listener closes a socket that never starts its handshake
    Then connections max idle: a tls listener closes a socket that never starts its handshake
