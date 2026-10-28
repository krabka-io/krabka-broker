Feature: tests support relay

  Scenario: relay forwards bytes to the upstream
    Then support relay: relay forwards bytes to the upstream

  Scenario: cut tears down an open connection and stops new ones
    Then support relay: cut tears down an open connection and stops new ones

  Scenario: heal restores forwarding without a restart
    Then support relay: heal restores forwarding without a restart

  Scenario: shutdown ends the relay with a connection still open
    Then support relay: shutdown ends the relay with a connection still open

  Scenario: cut and heal work through a shared relay
    Then support relay: cut and heal work through a shared relay

  Scenario: site link cuts and heals both listeners together
    Then support relay: site link cuts and heals both listeners together
