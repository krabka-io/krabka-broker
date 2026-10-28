Feature: Kafka feature update compatibility and diagnostic precedence
  Node support ranges use Kafka diagnostics. The active controller validates
  its own ranges before broker registrations, then controller registrations
  and missing voters when the metadata version supports them.

  Scenario Outline: Broker range refusals use the exact Kafka version-range text
    Given Broker 2 supports feature levels <minimum> through <maximum>
    And the requested feature level is <level>
    When Broker 2 checks the requested feature level against its range
    Then the feature compatibility diagnostic is <message>

    Examples:
      | minimum | maximum | level | message |
      | 0 | 1 | 1 | null |
      | 0 | 1 | 9 | "Broker 2 only supports versions 0-1" |
      | 1 | 1 | 2 | "Broker 2 only supports versions 1" |
      | 0 | 0 | 1 | "Broker 2 does not support this feature." |
      | 0 | 0 | 0 | null |

  Scenario: Every registered node supports the requested feature
    Given the feature-update cluster uses metadata version 30
    And broker 2 advertises group.version as present
    When controller 1 checks cluster support for group.version level 1
    Then the feature compatibility diagnostic is null

  Scenario: Unknown features fail locally before checking a broker
    Given the feature-update cluster uses metadata version 30
    And broker 2 advertises group.version as absent
    When controller 1 checks cluster support for no.such.feature level 1
    Then the feature compatibility diagnostic is "Local controller 1 does not support this feature."

  Scenario: Local range refusals precede registration checks
    Given the feature-update cluster uses metadata version 30
    When controller 1 checks cluster support for group.version level 9
    Then the feature compatibility diagnostic is "Local controller 1 only supports versions 0-1"

  Scenario: A registered broker without the feature prevents the update
    Given the feature-update cluster uses metadata version 30
    And broker 2 advertises group.version as absent
    When controller 1 checks cluster support for group.version level 1
    Then the feature compatibility diagnostic is "Broker 2 does not support this feature."

  Scenario: A registered peer controller without the feature prevents the update
    Given the feature-update cluster uses metadata version 30
    And controller 3 advertises group.version as absent
    When controller 1 checks cluster support for group.version level 1
    Then the feature compatibility diagnostic is "Controller 3 does not support this feature."

  Scenario: The local controller uses its supported ranges rather than its own registration
    Given the feature-update cluster uses metadata version 30
    And controller 1 advertises group.version as absent
    When controller 1 checks cluster support for group.version level 1
    Then the feature compatibility diagnostic is null

  Scenario: Older metadata versions ignore controller registration and missing voters
    Given the feature-update cluster uses metadata version 14
    And controller 3 advertises group.version as absent
    And the feature-update quorum voters are 1,4
    When controller 1 checks cluster support for group.version level 1
    Then the feature compatibility diagnostic is null

  Scenario: A missing voter registration is reported after registered peers pass
    Given the feature-update cluster uses metadata version 30
    And controller 3 advertises group.version as present
    And the feature-update quorum voters are 1,3,4
    When controller 1 checks cluster support for group.version level 1
    Then the feature compatibility diagnostic is "controller 4 has not registered, and may not support this feature"
