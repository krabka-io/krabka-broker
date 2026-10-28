Feature: tests kip590 envelope

  Scenario: a forwarded create topics is served and answered in the clients own bytes
    Then kip590 envelope: a forwarded create topics is served and answered in the clients own bytes

  Scenario: an envelope the controller refuses reports kafkas own error code
    Then kip590 envelope: an envelope the controller refuses reports kafkas own error code

  Scenario: envelope is advertised on the controller listener and nowhere else
    Then kip590 envelope: envelope is advertised on the controller listener and nowhere else

  Scenario: a forwarded request authorizes against the embedded client host
    Then kip590 envelope: a forwarded request authorizes against the embedded client host

  Scenario: an unparseable client host address is an invalid request
    Then kip590 envelope: an unparseable client host address is an invalid request

  Scenario: a forwarded allocate producer ids needs cluster action for the embedded principal
    Then kip590 envelope: a forwarded allocate producer ids needs cluster action for the embedded principal

  Scenario: an envelope from a peer without cluster action is refused
    Then kip590 envelope: an envelope from a peer without cluster action is refused

  Scenario: an embedded version the broker does not serve is refused
    Then kip590 envelope: an embedded version the broker does not serve is refused

  Scenario: an embedded request the controller cannot parse is answered in the envelope
    Then kip590 envelope: an embedded request the controller cannot parse is answered in the envelope

  Scenario: an embedded trunk api key is refused unless unstable api versions are enabled
    Then kip590 envelope: an embedded trunk api key is refused unless unstable api versions are enabled

  Scenario: a forwarded request carries the clients own delegation token flag
    Then kip590 envelope: a forwarded request carries the clients own delegation token flag
