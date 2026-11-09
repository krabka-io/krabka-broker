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
      Given an authorized upsert of a <mechanism> credential with <iterations> iterations
      When the SCRAM upsert is validated
      Then the alteration is accepted as a <mechanism> credential

      Examples:
        | case                   | mechanism     | iterations |
        | at the minimum allowed | SCRAM-SHA-256 | 4096       |
        | at the maximum allowed | SCRAM-SHA-512 | 16384      |

  Rule: A deletion needs an existing credential and no iteration count

    Scenario: Deleting a credential that does not exist is refused
      Given a deletion of a SCRAM-SHA-256 credential that does not exist
      When the deletion is validated
      Then the deletion is refused because no such credential exists

    Scenario Outline: Deleting an existing credential is accepted without an iteration count
      Given a deletion of an existing <mechanism> credential
      When the deletion is validated
      Then the alteration is accepted as a <mechanism> credential

      Examples:
        | case               | mechanism     |
        | the 256-bit digest | SCRAM-SHA-256 |
        | the 512-bit digest | SCRAM-SHA-512 |
