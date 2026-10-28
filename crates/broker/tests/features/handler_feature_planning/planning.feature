Feature: UpdateFeatures validates the proposed feature graph and writes complete Kafka plans
  Scenario: Accepted rows use their last value and proposed dependency levels: levels=metadata.version=25, updates=none, records=none (1)
    Given the finalized feature levels are metadata.version=25
    And the requested feature rows are none
    When the local controller plans the validate-only feature request
    Then the complete feature plan writes none reports features none upgrades kraft to none and enables ELR false

  Scenario: Accepted rows use their last value and proposed dependency levels: levels=metadata.version=25, updates=group.version:9:1;group.version:1:1, records=group.version=1 (2)
    Given the finalized feature levels are metadata.version=25
    And the requested feature rows are group.version:9:1;group.version:1:1
    When the local controller plans the validate-only feature request
    Then the complete feature plan writes group.version=1 reports features group.version upgrades kraft to none and enables ELR false

  Scenario: Accepted rows use their last value and proposed dependency levels: levels=metadata.version=25, updates=share.version:0:2, records=share.version=0 (3)
    Given the finalized feature levels are metadata.version=25
    And the requested feature rows are share.version:0:2
    When the local controller plans the validate-only feature request
    Then the complete feature plan writes share.version=0 reports features share.version upgrades kraft to none and enables ELR false

  Scenario: Accepted rows use their last value and proposed dependency levels: levels=metadata.version=22, updates=metadata.version:23:1;eligible.leader.replicas.version:1:1, records=metadata.version=23,eligible.leader.replicas.version=1 (4)
    Given the finalized feature levels are metadata.version=22
    And the requested feature rows are metadata.version:23:1;eligible.leader.replicas.version:1:1
    When the local controller plans the validate-only feature request
    Then the complete feature plan writes metadata.version=23,eligible.leader.replicas.version=1 reports features metadata.version,eligible.leader.replicas.version upgrades kraft to none and enables ELR true

  Scenario: Accepted rows use their last value and proposed dependency levels: levels=metadata.version=25, updates=kraft.version:1:1, records=none (5)
    Given the finalized feature levels are metadata.version=25
    And the requested feature rows are kraft.version:1:1
    When the local controller plans the validate-only feature request
    Then the complete feature plan writes none reports features kraft.version upgrades kraft to 1 and enables ELR false

  Scenario: Accepted rows use their last value and proposed dependency levels: levels=metadata.version=25, updates=kraft.version:0:1, records=none (6)
    Given the finalized feature levels are metadata.version=25
    And the requested feature rows are kraft.version:0:1
    When the local controller plans the validate-only feature request
    Then the complete feature plan writes none reports features kraft.version upgrades kraft to none and enables ELR false

  Scenario Outline: Invalid feature rows retain complete Kafka refusal diagnostics
    Given the finalized feature levels are <levels>
    And the requested feature rows are <feature>:<level>:<kind>
    When the local controller plans the validate-only feature request
    Then feature <feature> level <level> is refused because <reason>
    Examples:
      | levels                              | feature                          | level | kind | reason                                                                                                                                       |
      | metadata.version=25                 | no.such.feature                  | 1     | 1    | Local controller 1 does not support this feature.                                                                                            |
      | metadata.version=25                 | no.such.feature                  | 0     | 2    | Feature no.such.feature not found.                                                                                                           |
      | metadata.version=25                 | group.version                    | 1     | 0    | The controller does not support the given upgrade type.                                                                                      |
      | metadata.version=25                 | group.version                    | -1    | 1    | A feature version cannot be less than 0.                                                                                                     |
      | metadata.version=25                 | share.version                    | 9     | 1    | Local controller 1 only supports versions 0-1                                                                                                |
      | metadata.version=25,group.version=1 | group.version                    | 0     | 1    | Can't downgrade the version of this feature without setting the upgrade type to either safe or unsafe downgrade.                            |
      | metadata.version=25                 | group.version                    | 1     | 2    | Can't downgrade to a newer version.                                                                                                         |
      | metadata.version=22                 | eligible.leader.replicas.version | 1     | 1    | eligible.leader.replicas.version could not be set to 1 because it depends on metadata.version level 23                                         |
      | metadata.version=25                 | kraft.version                    | 1     | 2    | Can't downgrade to a newer version.                                                                                                         |
      | metadata.version=25                 | kraft.version                    | 2     | 1    | Local controller 1 only supports versions 0-1                                                                                                |

  Scenario Outline: Krabka feature levels use the same non-metadata finalization rules
    Given the finalized feature levels are <levels>
    And the requested feature rows are krabka.version:<level>:<kind>
    When the local controller plans the validate-only feature request
    Then the complete feature plan writes krabka.version=<level> reports features krabka.version upgrades kraft to none and enables ELR false
    Examples:
      | levels                               | level | kind |
      | metadata.version=30                  | 1     | 1    |
      | metadata.version=30,krabka.version=1 | 1     | 1    |
      | metadata.version=30,krabka.version=1 | 0     | 2    |
      | metadata.version=30,krabka.version=1 | 0     | 3    |

  Scenario Outline: Registered brokers must support the proposed Krabka feature level
    Given the finalized feature levels are <levels>
    And registered broker 2 advertises krabka.version range <range>
    And the requested feature rows are krabka.version:<level>:<kind>
    When the local controller plans the validate-only feature request
    Then the complete feature plan writes krabka.version=<level> reports features krabka.version upgrades kraft to none and enables ELR false
    Examples:
      | levels                               | range | level | kind |
      | metadata.version=30                  | 0-1   | 1     | 1    |
      | metadata.version=30,krabka.version=1 | 0-0   | 0     | 2    |

  Scenario Outline: Missing or narrow registered broker support refuses the Krabka feature
    Given the finalized feature levels are metadata.version=30
    And registered broker 2 advertises krabka.version range <range>
    And the requested feature rows are krabka.version:1:1
    When the local controller plans the validate-only feature request
    Then feature krabka.version level 1 is refused because Broker 2 does not support this feature.
    Examples:
      | range   |
      | 0-0     |
      | missing |

  Scenario Outline: Local support and explicit downgrade intent bound Krabka feature changes
    Given the finalized feature levels are <levels>
    And the requested feature rows are krabka.version:<level>:<kind>
    When the local controller plans the validate-only feature request
    Then feature krabka.version level <level> is refused because <reason>
    Examples:
      | levels                               | level | kind | reason                                                                                                                |
      | metadata.version=30                  | 3     | 1    | Local controller 1 only supports versions 0-2                                                                         |
      | metadata.version=30,krabka.version=1 | 0     | 1    | Can't downgrade the version of this feature without setting the upgrade type to either safe or unsafe downgrade.     |
      | metadata.version=30                  | 1     | 2    | Can't downgrade to a newer version.                                                                                  |

  Scenario Outline: Enabling ELR establishes cluster minimum ISR and clears all per-broker overrides
    Given ELR is disabled with cluster minimum ISR <cluster> broker <broker> minimum ISR <value> and static minimum ISR <static_value>
    When enabling ELR generates its minimum ISR safety records
    Then the ordered minimum ISR safety records are <records>
    Examples:
      | cluster | broker | value | static_value | records          |
      | unset   | 1      | unset | 2            | cluster=2        |
      | 3       | 1      | unset | 2            | none             |
      | unset   | 1      | 2     | 1            | cluster=1,1=null |
      | 3       | 1      | 2     | 1            | 1=null           |
      | 3       | 7      | 2     | 1            | 7=null           |
