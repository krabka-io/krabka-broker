Feature: Fault-injection links cut live connections and heal in place
  Scenario: relay forwards bytes to the upstream
    Given a loopback echo server is reachable through a relay
    When a fresh connection sends ping through the relay
    Then the relay binds a separate port from its upstream
    And the relay echoes exactly ping

  Scenario: cut tears down an open connection and stops new ones
    Given a loopback echo server is reachable through a relay
    When a connection opens and sends before through the relay
    Then the relay echoes exactly before
    When the relay link is cut
    And the already open connection sends x
    Then the relay refuses to carry the connection
    When a fresh connection sends x through the relay
    Then the relay refuses to carry the connection

  Scenario: heal restores forwarding without a restart
    Given a loopback echo server is reachable through a relay
    When the relay link is cut
    And a fresh connection sends x through the relay
    Then the relay refuses to carry the connection
    When the relay link heals
    Then the healed relay retains its original listener address
    When a fresh connection sends after through the relay
    Then the relay echoes exactly after

  Scenario: shutdown ends the relay with a connection still open
    Given a loopback echo server is reachable through a relay
    When a connection opens and sends open through the relay
    Then the relay echoes exactly open
    When the relay shuts down while its connection remains open
    And the already open connection sends x
    Then the relay refuses to carry the connection
    When a fresh connection sends x through the relay
    Then the relay refuses to carry the connection

  Scenario: cut and heal work through a shared relay
    Given a loopback echo server is reachable through a relay
    When another task cuts the shared relay
    And a fresh connection sends x through the relay
    Then the relay refuses to carry the connection
    When another task heals the shared relay
    And a fresh connection sends after through the relay
    Then the relay echoes exactly after

  Scenario: site link cuts and heals both listeners together
    Given a site link connects separate controller and client echo servers
    When the controller sends ctrl and the client sends data through the site
    Then the distinct site listeners echo ctrl and data respectively
    When the whole site link is cut
    And both site listeners receive an attempted x round trip
    Then neither site listener carries the attempted round trip
    When the whole site link heals
    And the controller sends ctrl and the client sends data through the site
    Then the distinct site listeners echo ctrl and data respectively
