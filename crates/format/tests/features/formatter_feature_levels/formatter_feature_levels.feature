Feature: Formatter feature-level resolution follows Kafka validation order
  Scenario Outline: Invalid feature levels retain Kafka diagnostics
    Given formatter release selection <release>
    And formatter feature selections <features>
    And unstable formatter features are <unstable>
    When formatter feature selections are resolved
    Then the formatter feature error is <error>

    Examples:
      | case                                        | release | features | unstable | error |
      | a group level above the latest              | default | group.version=5 | false | No feature:group.version with feature level 5 |
      | a transaction level above the latest        | default | transaction.version=3 | false | No feature:transaction.version with feature level 3 |
      | a transaction level far above the latest    | default | transaction.version=9 | false | No feature:transaction.version with feature level 9 |
      | a negative group level                      | default | group.version=-1 | false | No feature:group.version with feature level -1 |
      | a metadata level below the minimum          | default | metadata.version=6 | false | No MetadataVersion with feature level 6. Valid feature levels are from 7 to 34. |
      | a metadata level far below the minimum      | default | metadata.version=1 | false | No MetadataVersion with feature level 1. Valid feature levels are from 7 to 34. |
      | an unstable metadata level                  | default | metadata.version=34 | false | metadata.version 4.5-IV0 is not yet stable. |
      | a metadata level above the latest           | default | metadata.version=99 | false | No MetadataVersion with feature level 99. Valid feature levels are from 7 to 34. |
      | unstable features keep the minimum          | default | metadata.version=6 | true | No MetadataVersion with feature level 6. Valid feature levels are from 7 to 34. |
      | a metadata level above the unstable latest  | default | metadata.version=35 | true | No MetadataVersion with feature level 35. Valid feature levels are from 7 to 34. |
      | a metadata feature beside a release         | 4.0-IV0 | metadata.version=24 | false | Use --release-version instead of --feature metadata.version=X to avoid ambiguity. |
      | an unknown metadata level beside a release  | 4.0-IV0 | metadata.version=99 | false | Use --release-version instead of --feature metadata.version=X to avoid ambiguity. |
      | a bad metadata level beats an unknown name  | default | bogus.version=1,metadata.version=99 | false | No MetadataVersion with feature level 99. Valid feature levels are from 7 to 34. |
      | a krabka level above the latest             | default | krabka.version=4 | false | No feature:krabka.version with feature level 4 |

  Scenario Outline: Unsupported feature names precede invalid known feature levels
    Given formatter release selection default
    And formatter feature selections <features>
    And unstable formatter features are <unstable>
    When formatter feature selections are resolved
    Then the unsupported feature error lists every supported feature in Kafka order

    Examples:
      | case                                    | features                       | unstable |
      | an unknown name alone                   | bogus.version=1                | false    |
      | an unknown name with unstable features  | bogus.version=1                | true     |
      | an unknown name after a bad known level | group.version=5,bogus.version=1 | false    |

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
      | case                        | features         |
      | no krabka feature requested | none             |
      | an explicit level zero      | krabka.version=0 |

  Scenario: A finalized Krabka feature level is seeded explicitly
    Given formatter release selection default
    And formatter feature selections krabka.version=1
    And unstable formatter features are false
    When the formatter builds bootstrap feature records
    Then bootstrap preserves release defaults and adds krabka.version level 1
