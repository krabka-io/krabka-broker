Feature: Listener idle timeouts reclaim silent peers and reset on incoming bytes
  Scenario Outline: silent plaintext connection closes past its idle window
    Given a <scope> idle timeout of <window_secs> seconds on a plaintext listener
    When a peer connects and the plaintext listener arms its idle timer
    Then exactly 0 connections have been counted as idle closes
    When the clock advances <advance_secs> seconds without client traffic
    Then the silent connection closes without a response
    And exactly 1 connections have been counted as idle closes
    Examples:
      | scope | window_secs | advance_secs |
      | broker-wide | 60 | 61 |
      | listener-override | 20 | 21 |

  Scenario: a connection speaking each half window is never closed
    Given a broker-wide idle timeout of 60 seconds on a plaintext listener
    When a peer connects and the plaintext listener arms its idle timer
    And the client sends API versions requests every 30 seconds for four half-windows
    Then all four API versions requests are served successfully
    And exactly 0 connections have been counted as idle closes

  Scenario: a request trickled across two windows is served
    Given a broker-wide idle timeout of 60 seconds on a plaintext listener
    And an API versions v0 frame with correlation id 7
    When a peer connects and the plaintext listener arms its idle timer
    And the client trickles the frame in four chunks separated by 30 seconds
    And the client reads the trickled request response
    Then the trickled request succeeds with correlation id 7
    And exactly 0 connections have been counted as idle closes

  Scenario: a tls listener closes a socket that never starts its handshake
    Given a TLS listener with an idle timeout of 250 milliseconds
    When a peer opens a TCP connection without starting TLS
    Then the silent connection closes without a response
    And exactly 1 connections have been counted as idle closes
