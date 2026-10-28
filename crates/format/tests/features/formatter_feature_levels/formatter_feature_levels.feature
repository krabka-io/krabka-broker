Feature: Formatter feature-level resolution follows Kafka validation order
  Scenario Outline: Invalid feature levels retain Kafka diagnostics
    Given formatter release selection <release>
    And formatter feature selections <features>
    And unstable formatter features are <unstable>
    When formatter feature selections are resolved
    Then the formatter feature error is <error>

    Examples:
      | release | features | unstable | error |
      | default | group.version=5 | false | No feature:group.version with feature level 5 |
      | default | transaction.version=3 | false | No feature:transaction.version with feature level 3 |
      | default | transaction.version=9 | false | No feature:transaction.version with feature level 9 |
      | default | group.version=-1 | false | No feature:group.version with feature level -1 |
      | default | metadata.version=6 | false | No MetadataVersion with feature level 6. Valid feature levels are from 7 to 31. |
      | default | metadata.version=1 | false | No MetadataVersion with feature level 1. Valid feature levels are from 7 to 31. |
      | default | metadata.version=32 | false | No MetadataVersion with feature level 32. Valid feature levels are from 7 to 31. |
      | default | metadata.version=33 | false | No MetadataVersion with feature level 33. Valid feature levels are from 7 to 31. |
      | default | metadata.version=34 | false | No MetadataVersion with feature level 34. Valid feature levels are from 7 to 31. |
      | default | metadata.version=99 | false | No MetadataVersion with feature level 99. Valid feature levels are from 7 to 31. |
      | default | metadata.version=31 | false | metadata.version 4.4-IV0 is not yet stable. |
      | default | metadata.version=6 | true | No MetadataVersion with feature level 6. Valid feature levels are from 7 to 34. |
      | default | metadata.version=35 | true | No MetadataVersion with feature level 35. Valid feature levels are from 7 to 34. |
      | 4.0-IV0 | metadata.version=24 | false | Use --release-version instead of --feature metadata.version=X to avoid ambiguity. |
      | 4.0-IV0 | metadata.version=99 | false | Use --release-version instead of --feature metadata.version=X to avoid ambiguity. |
      | default | bogus.version=1,metadata.version=99 | false | No MetadataVersion with feature level 99. Valid feature levels are from 7 to 31. |
      | default | krabka.version=3 | false | No feature:krabka.version with feature level 3 |

  Scenario Outline: Unsupported feature names precede invalid known feature levels
    Given formatter release selection default
    And formatter feature selections <features>
    And unstable formatter features are <unstable>
    When formatter feature selections are resolved
    Then the unsupported feature error lists every supported feature in Kafka order

    Examples:
      | features                       | unstable |
      | bogus.version=1                | false    |
      | bogus.version=1                | true     |
      | group.version=5,bogus.version=1 | false    |

  Scenario: An unknown release is rejected before unknown feature names
    Given formatter release selection 9.9
    And formatter feature selections bogus.version=1
    And unstable formatter features are false
    When formatter feature selections are resolved
    Then unknown release 9.9 is reported before an unsupported feature

  Scenario Outline: Default and explicit level-zero Krabka features do not emit a record
    Given formatter release selection default
    And formatter feature selections <features>
    And unstable formatter features are false
    When the formatter builds bootstrap feature records
    Then bootstrap preserves release defaults and omits the level-zero krabka.version record

    Examples:
      | features         |
      | none             |
      | krabka.version=0 |

  Scenario: A finalized Krabka feature level is seeded explicitly
    Given formatter release selection default
    And formatter feature selections krabka.version=1
    And unstable formatter features are false
    When the formatter builds bootstrap feature records
    Then bootstrap preserves release defaults and adds krabka.version level 1
