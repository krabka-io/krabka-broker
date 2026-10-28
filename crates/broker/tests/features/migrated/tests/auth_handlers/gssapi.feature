Feature: GSSAPI discovery and keytab authentication
  Scenario: gssapi handshake advertised when enabled
    Given a GSSAPI listener without a running KDC
    When an unauthenticated client discovers APIs and selects GSSAPI
    Then GSSAPI selection succeeds and advertises GSSAPI without contacting a KDC

  Scenario: gssapi inter broker client authenticates from keytab
    Given a live MIT KDC and broker and alice service keytabs
    When the inter broker client authenticates from alice keytab and requests API versions
    Then the keytab authenticated connection serves API versions
