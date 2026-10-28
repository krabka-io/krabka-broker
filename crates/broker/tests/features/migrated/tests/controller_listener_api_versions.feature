Feature: Controller listeners preserve authentication independent discovery and socket limits
  Scenario Outline: sasl controller listener answers api versions before authentication
    Given an unauthenticated PLAIN connection to a SASL controller listener
    When the client sends unsupported API versions v5 with an invalid body
    Then the controller returns the exact v0 refusal advertising API versions 0 through 4
    When the unauthenticated client requests API versions v<version>
    And the client authenticates as broker with PLAIN
    Then both PLAIN handshake and authentication return success
    When the authenticated client requests API versions v<version>
    Then the v<version> controller API and feature table is unchanged and its epoch does not regress
    Examples:
      | version |
      | 0 |
      | 3 |
      | 4 |

  Scenario: the controller listener refuses a request over socket request max
    Given a PLAINTEXT controller with a 4 KiB request size limit
    When the client requests API versions v0 on the bounded controller
    Then the bounded controller serves the valid request
    When the client declares a 1 MiB frame without sending its body
    Then the controller closes the oversize connection without returning bytes

  Scenario: the controller listener closes an idle connection
    Given a PLAINTEXT controller with an 800 millisecond idle timeout and two clients
    When one client remains idle while the other requests API versions every 200 milliseconds for 1600 milliseconds
    Then every request on the active client succeeds
    And the idle client is closed without returning bytes
