Feature: SCRAM credential alteration error precedence
  Existing row outcomes take precedence over authorization, name, mechanism and
  iteration validation. A later error must not replace the earlier failure.

  Scenario: Validation returns the first applicable SCRAM error (1: prior rejected, authorized false)
    Given a SCRAM upsert with prior result rejected, authorization false, empty name true, mechanism 0, and iterations minimum-integer
    When the SCRAM upsert is validated
    Then the SCRAM response preserves the-prior-error

  Scenario: Validation returns the first applicable SCRAM error (2: prior accepted, authorized false)
    Given a SCRAM upsert with prior result accepted, authorization false, empty name true, mechanism 0, and iterations minimum-integer
    When the SCRAM upsert is validated
    Then the SCRAM response preserves a-duplicate-error

  Scenario: Validation returns the first applicable SCRAM error (3: prior unseen, authorized false)
    Given a SCRAM upsert with prior result unseen, authorization false, empty name true, mechanism 0, and iterations minimum-integer
    When the SCRAM upsert is validated
    Then the SCRAM response preserves an-authorization-error

  Scenario: Validation returns the first applicable SCRAM error (4: prior unseen, authorized true)
    Given a SCRAM upsert with prior result unseen, authorization true, empty name true, mechanism 0, and iterations minimum-integer
    When the SCRAM upsert is validated
    Then the SCRAM response preserves an-empty-name-error

  Scenario: Validation returns the first applicable SCRAM error (5: prior unseen, authorized true)
    Given a SCRAM upsert with prior result unseen, authorization true, empty name false, mechanism 0, and iterations minimum-integer
    When the SCRAM upsert is validated
    Then the SCRAM response preserves an-unsupported-mechanism-error

  Scenario: Validation returns the first applicable SCRAM error (6: prior unseen, authorized true)
    Given a SCRAM upsert with prior result unseen, authorization true, empty name false, mechanism 1, and iterations minimum-integer
    When the SCRAM upsert is validated
    Then the SCRAM response preserves a-too-few-iterations-error

  Scenario: Validation returns the first applicable SCRAM error (7: prior unseen, authorized true)
    Given a SCRAM upsert with prior result unseen, authorization true, empty name false, mechanism 1, and iterations maximum-integer
    When the SCRAM upsert is validated
    Then the SCRAM response preserves a-too-many-iterations-error
