Feature: A broker starts only with the audit checkpoint signing it was configured for

  Rule: A configured audit signing key that does not load stops the broker in every failure mode

    Scenario Outline: A missing audit signing key file refuses startup
      Given the broker audits in <mode> mode
      And the audit signing key file the broker names does not exist
      When the broker attempts to start
      Then startup is refused because the audit signing key did not load

      Examples:
        | case        | mode        |
        | fail-open   | fail-open   |
        | fail-closed | fail-closed |

    Scenario: A file that holds no Ed25519 key refuses startup
      Given the broker audits in fail-open mode
      And the audit signing key file the broker names holds no key
      When the broker attempts to start
      Then startup is refused because the audit signing key did not load

  Rule: A fail-closed broker needs an audit signing key

    Scenario: A fail-closed broker without an audit signing key refuses to start
      Given the broker audits in fail-closed mode
      When the broker attempts to start
      Then startup is refused because a fail-closed broker needs an audit signing key

    Scenario: A fail-closed broker with an audit signing key signs its checkpoints
      Given the broker audits in fail-closed mode
      And the broker names an Ed25519 audit signing key
      When the broker attempts to start
      Then the broker starts with checkpoint signing enabled

  Rule: A fail-open broker without an audit signing key starts unsigned

    Scenario: A fail-open broker without an audit signing key starts with checkpoints disabled
      Given the broker audits in fail-open mode
      When the broker attempts to start
      Then the broker starts with checkpoint signing disabled
