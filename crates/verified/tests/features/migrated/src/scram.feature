Feature: SCRAM credential alteration validation
  Existing row outcomes take precedence over authorization, name, mechanism and
  iteration validation. A later error must not replace the earlier failure.

  Rule: Validation returns the first applicable SCRAM error

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

  Rule: A complete upsert is accepted under its mechanism

    Scenario Outline: An upsert within the iteration bounds is accepted
      Given a SCRAM upsert with prior result unseen, authorization true, empty name false, mechanism <mechanism>, and iterations <iterations>
      When the SCRAM upsert is validated
      Then the SCRAM alteration is accepted as <accepted>

      Examples:
        | case                                    | mechanism | iterations | accepted      |
        | SCRAM-SHA-256 at the minimum iterations | 1         | 4096       | SCRAM-SHA-256 |
        | SCRAM-SHA-512 at the maximum iterations | 2         | 16384      | SCRAM-SHA-512 |

  Rule: A deletion needs an existing credential and no iteration count

    Scenario: Deleting a credential that does not exist is refused
      Given a SCRAM deletion of a mechanism 1 credential that does not exist
      When the SCRAM deletion is validated
      Then the SCRAM response preserves a-missing-credential-error

    Scenario Outline: Deleting an existing credential is accepted without an iteration count
      Given a SCRAM deletion of an existing mechanism <mechanism> credential
      When the SCRAM deletion is validated
      Then the SCRAM alteration is accepted as <accepted>

      Examples:
        | case          | mechanism | accepted      |
        | SCRAM-SHA-256 | 1         | SCRAM-SHA-256 |
        | SCRAM-SHA-512 | 2         | SCRAM-SHA-512 |
