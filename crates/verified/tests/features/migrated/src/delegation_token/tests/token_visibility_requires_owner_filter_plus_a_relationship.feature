Feature: Delegation token creation and renewal deadlines
  Token deadlines use saturating signed-millisecond arithmetic. Requested periods
  are bounded by configured policy and the token maximum, and expiry is strict.

  Scenario: Creation applies configured lifetime and renewal policy with saturation (1: boundary negative request uses ceiling, ceiling 1000)
    Given a token policy ceiling of 1000 ms and renewal interval of 100 ms
    When a token is created at 0 ms with requested lifetime -1 ms
    Then token creation is create with maximum timestamp 1000 ms and initial expiry 100 ms

  Scenario: Creation applies configured lifetime and renewal policy with saturation (2: boundary later time adds deadlines, ceiling 1000)
    Given a token policy ceiling of 1000 ms and renewal interval of 100 ms
    When a token is created at 100 ms with requested lifetime -1 ms
    Then token creation is create with maximum timestamp 1100 ms and initial expiry 200 ms

  Scenario: Creation applies configured lifetime and renewal policy with saturation (3: boundary zero request uses ceiling, ceiling 1000)
    Given a token policy ceiling of 1000 ms and renewal interval of 100 ms
    When a token is created at 100 ms with requested lifetime 0 ms
    Then token creation is create with maximum timestamp 1100 ms and initial expiry 200 ms

  Scenario: Creation applies configured lifetime and renewal policy with saturation (4: boundary other negative request uses ceiling, ceiling 1000)
    Given a token policy ceiling of 1000 ms and renewal interval of 100 ms
    When a token is created at 100 ms with requested lifetime -2 ms
    Then token creation is create with maximum timestamp 1100 ms and initial expiry 200 ms

  Scenario: Creation applies configured lifetime and renewal policy with saturation (5: boundary minimum request uses ceiling, ceiling 1000)
    Given a token policy ceiling of 1000 ms and renewal interval of 100 ms
    When a token is created at 100 ms with requested lifetime minimum ms
    Then token creation is create with maximum timestamp 1100 ms and initial expiry 200 ms

  Scenario: Creation applies configured lifetime and renewal policy with saturation (6: boundary positive request caps lifetime, ceiling 1000)
    Given a token policy ceiling of 1000 ms and renewal interval of 100 ms
    When a token is created at 100 ms with requested lifetime 50 ms
    Then token creation is create with maximum timestamp 150 ms and initial expiry 150 ms

  Scenario: Creation applies configured lifetime and renewal policy with saturation (7: boundary oversized request uses ceiling, ceiling 1000)
    Given a token policy ceiling of 1000 ms and renewal interval of 100 ms
    When a token is created at 100 ms with requested lifetime 5000 ms
    Then token creation is create with maximum timestamp 1100 ms and initial expiry 200 ms

  Scenario: Creation applies configured lifetime and renewal policy with saturation (8: boundary renewal period can extend expiry, ceiling 100)
    Given a token policy ceiling of 100 ms and renewal interval of 101 ms
    When a token is created at 100 ms with requested lifetime -1 ms
    Then token creation is create with maximum timestamp 200 ms and initial expiry 200 ms

  Scenario: Creation applies configured lifetime and renewal policy with saturation (9: boundary maximum expiry saturates, ceiling 9223372036854775707)
    Given a token policy ceiling of 9223372036854775707 ms and renewal interval of 100 ms
    When a token is created at 100 ms with requested lifetime -1 ms
    Then token creation is create with maximum timestamp maximum ms and initial expiry 200 ms

  Scenario: Creation applies configured lifetime and renewal policy with saturation (10: boundary both deadlines saturate, ceiling 1)
    Given a token policy ceiling of 1 ms and renewal interval of 1 ms
    When a token is created at maximum ms with requested lifetime -1 ms
    Then token creation is create with maximum timestamp maximum ms and initial expiry maximum ms

  Scenario: Creation applies configured lifetime and renewal policy with saturation (11: boundary only expiry saturates, ceiling maximum)
    Given a token policy ceiling of maximum ms and renewal interval of 1 ms
    When a token is created at 1 ms with requested lifetime -1 ms
    Then token creation is create with maximum timestamp maximum ms and initial expiry 2 ms

  Scenario: Creation applies configured lifetime and renewal policy with saturation (12: boundary expiry and renewal saturate, ceiling maximum)
    Given a token policy ceiling of maximum ms and renewal interval of maximum ms
    When a token is created at 1 ms with requested lifetime -1 ms
    Then token creation is create with maximum timestamp maximum ms and initial expiry maximum ms

  Scenario: Creation applies configured lifetime and renewal policy with saturation (13: boundary zero ceiling is invalid, ceiling 0)
    Given a token policy ceiling of 0 ms and renewal interval of 100 ms
    When a token is created at 100 ms with requested lifetime -1 ms
    Then token creation is invalid with maximum timestamp none ms and initial expiry none ms

  Scenario: Creation applies configured lifetime and renewal policy with saturation (14: boundary zero renewal is invalid, ceiling 1000)
    Given a token policy ceiling of 1000 ms and renewal interval of 0 ms
    When a token is created at 100 ms with requested lifetime -1 ms
    Then token creation is invalid with maximum timestamp none ms and initial expiry none ms

  Scenario: Creation applies configured lifetime and renewal policy with saturation (15: boundary negative ceiling is invalid, ceiling -1)
    Given a token policy ceiling of -1 ms and renewal interval of 100 ms
    When a token is created at 100 ms with requested lifetime -1 ms
    Then token creation is invalid with maximum timestamp none ms and initial expiry none ms

  Scenario: Renewal caps the requested period and refuses expired tokens (1: boundary requested shorter period, default 50)
    Given a token expiring at 150 ms with maximum timestamp 1000 ms and default renewal interval 50 ms
    When renewal is requested at 100 ms for a period of 25 ms
    Then token renewal is renewed with expiry 125 ms

  Scenario: Renewal caps the requested period and refuses expired tokens (2: boundary requested longer period, default 50)
    Given a token expiring at 150 ms with maximum timestamp 1000 ms and default renewal interval 50 ms
    When renewal is requested at 100 ms for a period of 75 ms
    Then token renewal is renewed with expiry 150 ms

  Scenario: Renewal caps the requested period and refuses expired tokens (3: boundary requested much longer period, default 50)
    Given a token expiring at 150 ms with maximum timestamp 1000 ms and default renewal interval 50 ms
    When renewal is requested at 100 ms for a period of 500 ms
    Then token renewal is renewed with expiry 150 ms

  Scenario: Renewal caps the requested period and refuses expired tokens (4: boundary negative period uses default, default 50)
    Given a token expiring at 150 ms with maximum timestamp 1000 ms and default renewal interval 50 ms
    When renewal is requested at 100 ms for a period of -1 ms
    Then token renewal is renewed with expiry 150 ms

  Scenario: Renewal caps the requested period and refuses expired tokens (5: boundary zero period uses default, default 50)
    Given a token expiring at 150 ms with maximum timestamp 1000 ms and default renewal interval 50 ms
    When renewal is requested at 100 ms for a period of 0 ms
    Then token renewal is renewed with expiry 150 ms

  Scenario: Renewal caps the requested period and refuses expired tokens (6: boundary other negative period uses default, default 50)
    Given a token expiring at 150 ms with maximum timestamp 1000 ms and default renewal interval 50 ms
    When renewal is requested at 100 ms for a period of -5 ms
    Then token renewal is renewed with expiry 150 ms

  Scenario: Renewal caps the requested period and refuses expired tokens (7: boundary short period can shorten expiry, default 50)
    Given a token expiring at 900 ms with maximum timestamp 1000 ms and default renewal interval 50 ms
    When renewal is requested at 100 ms for a period of 10 ms
    Then token renewal is renewed with expiry 110 ms

  Scenario: Renewal caps the requested period and refuses expired tokens (8: boundary capped by maximum, default 1000)
    Given a token expiring at 150 ms with maximum timestamp 200 ms and default renewal interval 1000 ms
    When renewal is requested at 100 ms for a period of 500 ms
    Then token renewal is renewed with expiry 200 ms

  Scenario: Renewal caps the requested period and refuses expired tokens (9: boundary saturated expiry capped by maximum, default maximum)
    Given a token expiring at 150 ms with maximum timestamp 200 ms and default renewal interval maximum ms
    When renewal is requested at 100 ms for a period of maximum ms
    Then token renewal is renewed with expiry 200 ms

  Scenario: Renewal caps the requested period and refuses expired tokens (10: boundary saturated expiry, default maximum)
    Given a token expiring at 150 ms with maximum timestamp maximum ms and default renewal interval maximum ms
    When renewal is requested at 1 ms for a period of maximum ms
    Then token renewal is renewed with expiry maximum ms

  Scenario: Renewal caps the requested period and refuses expired tokens (11: boundary deadline equal to now is live, default 50)
    Given a token expiring at 100 ms with maximum timestamp 1000 ms and default renewal interval 50 ms
    When renewal is requested at 100 ms for a period of 25 ms
    Then token renewal is renewed with expiry 125 ms

  Scenario: Renewal caps the requested period and refuses expired tokens (12: boundary max equal to now caps renewal, default 50)
    Given a token expiring at 100 ms with maximum timestamp 100 ms and default renewal interval 50 ms
    When renewal is requested at 100 ms for a period of 25 ms
    Then token renewal is renewed with expiry 100 ms

  Scenario: Renewal caps the requested period and refuses expired tokens (13: boundary expired deadline, default 50)
    Given a token expiring at 99 ms with maximum timestamp 1000 ms and default renewal interval 50 ms
    When renewal is requested at 100 ms for a period of 25 ms
    Then token renewal is expired with expiry none ms

  Scenario: Renewal caps the requested period and refuses expired tokens (14: boundary max before now is expired, default 50)
    Given a token expiring at 150 ms with maximum timestamp 99 ms and default renewal interval 50 ms
    When renewal is requested at 100 ms for a period of 25 ms
    Then token renewal is expired with expiry none ms

  Scenario: Renewal caps the requested period and refuses expired tokens (15: boundary zero default cannot renew expired, default 0)
    Given a token expiring at 99 ms with maximum timestamp 1000 ms and default renewal interval 0 ms
    When renewal is requested at 100 ms for a period of 25 ms
    Then token renewal is expired with expiry none ms

  Scenario: Renewal caps the requested period and refuses expired tokens (16: boundary zero default is invalid while live, default 0)
    Given a token expiring at 150 ms with maximum timestamp 1000 ms and default renewal interval 0 ms
    When renewal is requested at 100 ms for a period of 25 ms
    Then token renewal is invalid with expiry none ms
