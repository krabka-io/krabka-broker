Feature: Authentication session expiry and reauthentication admission
  An ordinary frame on an expired session closes the connection. A SASL handshake
  may begin reauthentication instead, provided the connection has not started
  another reauthentication during the preceding second.

  Scenario: Expired sessions spare only a handshake that can begin reauthentication (1: boundary no deadline, expiry none)
    Given a session expiring at none ms with its last reauthentication at none ms
    When a Produce request arrives at maximum ms
    Then the request expires the authentication session: false

  Scenario: Expired sessions spare only a handshake that can begin reauthentication (2: boundary before deadline, expiry 1000)
    Given a session expiring at 1000 ms with its last reauthentication at none ms
    When a Produce request arrives at 999 ms
    Then the request expires the authentication session: false

  Scenario: Expired sessions spare only a handshake that can begin reauthentication (3: boundary at deadline, expiry 1000)
    Given a session expiring at 1000 ms with its last reauthentication at none ms
    When a Produce request arrives at 1000 ms
    Then the request expires the authentication session: false

  Scenario: Expired sessions spare only a handshake that can begin reauthentication (4: boundary after deadline, expiry 1000)
    Given a session expiring at 1000 ms with its last reauthentication at none ms
    When a Produce request arrives at 1001 ms
    Then the request expires the authentication session: true

  Scenario: Expired sessions spare only a handshake that can begin reauthentication (5: boundary ApiVersions after deadline, expiry 1000)
    Given a session expiring at 1000 ms with its last reauthentication at none ms
    When a ApiVersions request arrives at 1001 ms
    Then the request expires the authentication session: true

  Scenario: Expired sessions spare only a handshake that can begin reauthentication (6: boundary authentication at deadline, expiry 1000)
    Given a session expiring at 1000 ms with its last reauthentication at none ms
    When a SaslAuthenticate request arrives at 1000 ms
    Then the request expires the authentication session: false

  Scenario: Expired sessions spare only a handshake that can begin reauthentication (7: boundary authentication after deadline, expiry 1000)
    Given a session expiring at 1000 ms with its last reauthentication at none ms
    When a SaslAuthenticate request arrives at 1001 ms
    Then the request expires the authentication session: true

  Scenario: Expired sessions spare only a handshake that can begin reauthentication (8: boundary first reauthentication handshake, expiry 1000)
    Given a session expiring at 1000 ms with its last reauthentication at none ms
    When a SaslHandshake request arrives at 1001 ms
    Then the request expires the authentication session: false

  Scenario: Expired sessions spare only a handshake that can begin reauthentication (9: boundary handshake after one second, expiry 1000)
    Given a session expiring at 1000 ms with its last reauthentication at 1001 ms
    When a SaslHandshake request arrives at 2001 ms
    Then the request expires the authentication session: false

  Scenario: Expired sessions spare only a handshake that can begin reauthentication (10: boundary handshake inside one second, expiry 1000)
    Given a session expiring at 1000 ms with its last reauthentication at 1001 ms
    When a SaslHandshake request arrives at 2000 ms
    Then the request expires the authentication session: true

  Scenario: Expired sessions spare only a handshake that can begin reauthentication (11: boundary live session inside one second, expiry 1000)
    Given a session expiring at 1000 ms with its last reauthentication at 999 ms
    When a SaslHandshake request arrives at 1000 ms
    Then the request expires the authentication session: false

  Scenario: Expired sessions spare only a handshake that can begin reauthentication (12: boundary latest representable deadline, expiry maximum)
    Given a session expiring at maximum ms with its last reauthentication at none ms
    When a Produce request arrives at maximum ms
    Then the request expires the authentication session: false

  Scenario: Expired sessions spare only a handshake that can begin reauthentication (13: boundary earliest representable deadline, expiry minimum)
    Given a session expiring at minimum ms with its last reauthentication at none ms
    When a Produce request arrives at minimum-plus-one ms
    Then the request expires the authentication session: true

  Scenario: Expired sessions spare only a handshake that can begin reauthentication (14: boundary interval arithmetic saturates, expiry minimum)
    Given a session expiring at minimum ms with its last reauthentication at minimum ms
    When a SaslHandshake request arrives at maximum ms
    Then the request expires the authentication session: false

  Scenario Outline: Reauthentication requires an expiry and at least one second between starts
    Given a session expiring at <expiry> ms with its last reauthentication at <last_start> ms
    When reauthentication is attempted at <now> ms
    Then reauthentication can begin: <begins>

    Examples:
      | boundary                  | expiry | last_start | now     | begins |
      | session without an expiry | none   | none       | 5000    | false  |
      | first reauthentication    | 1000   | none       | 5000    | true   |
      | exactly one second later  | 1000   | 4000       | 5000    | true   |
      | one millisecond too soon  | 1000   | 4001       | 5000    | false  |
      | clock rollback            | 1000   | 6000       | 5000    | false  |
      | wide gap saturates        | 1000   | minimum    | maximum | true   |
      | wide rollback saturates   | 1000   | maximum    | minimum | false  |
