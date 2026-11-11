Feature: UpdateFeatures validates the proposed feature graph and writes complete Kafka plans

  Rule: An accepted request writes each feature at its last requested level, judged against the proposed dependency levels

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

  Rule: An invalid row refuses the whole request with Kafka's diagnostic for it

    Scenario Outline: Invalid feature rows retain complete Kafka refusal diagnostics
      Given the finalized feature levels are <levels>
      And the requested feature rows are <feature>:<level>:<kind>
      When the local controller plans the validate-only feature request
      Then feature <feature> level <level> is refused because <reason>
      Examples:
        | case                                       | levels                              | feature                          | level | kind | reason                                                                                                                                       |
        | an unknown feature upgrade                 | metadata.version=25                 | no.such.feature                  | 1     | 1    | Local controller 1 does not support this feature.                                                                                            |
        | an unknown feature downgrade               | metadata.version=25                 | no.such.feature                  | 0     | 2    | Feature no.such.feature not found.                                                                                                           |
        | an unknown upgrade type                    | metadata.version=25                 | group.version                    | 1     | 0    | The controller does not support the given upgrade type.                                                                                      |
        | a negative level                           | metadata.version=25                 | group.version                    | -1    | 1    | A feature version cannot be less than 0.                                                                                                     |
        | a level above the local range              | metadata.version=25                 | share.version                    | 9     | 1    | Local controller 1 only supports versions 0-2                                                                                                |
        | a downgrade without a downgrade type       | metadata.version=25,group.version=1 | group.version                    | 0     | 1    | Can't downgrade the version of this feature without setting the upgrade type to either safe or unsafe downgrade.                            |
        | a downgrade to a newer level               | metadata.version=25                 | group.version                    | 1     | 2    | Can't downgrade to a newer version.                                                                                                         |
        | a dependency on a newer metadata.version   | metadata.version=22                 | eligible.leader.replicas.version | 1     | 1    | eligible.leader.replicas.version could not be set to 1 because it depends on metadata.version level 23                                         |
        | a kraft.version downgrade to a newer level | metadata.version=25                 | kraft.version                    | 1     | 2    | Can't downgrade to a newer version.                                                                                                         |
        | a kraft.version above the local range      | metadata.version=25                 | kraft.version                    | 2     | 1    | Local controller 1 only supports versions 0-1                                                                                                |

  Rule: The first invalid row in Kafka's HashMap order names the refusal, not the first row of the request

    Scenario: A later row that fails first in HashMap order names the refusal
      Given the finalized feature levels are metadata.version=25
      And the requested feature rows are share.version:9:1;group.version:9:1
      When the local controller plans the validate-only feature request
      Then feature group.version level 9 is refused because Local controller 1 only supports versions 0-1

  Rule: A metadata.version downgrade is accepted only when no level it leaves changed the metadata

    Scenario Outline: A downgrade that leaves no metadata change writes only its feature record
      Given the finalized feature levels are metadata.version=<current>
      And the requested feature rows are metadata.version:<target>:<kind>
      When the local controller plans the validate-only feature request
      Then the complete feature plan writes metadata.version=<target> reports features metadata.version upgrades kraft to none and enables ELR false

      Examples:
        | case                                       | current | target | kind |
        | a safe downgrade to an unchanged level     | 25      | 24     | 2    |
        | an unsafe downgrade to an unchanged level  | 25      | 24     | 3    |
        | leaving 3.6-IV0, which changed nothing     | 12      | 11     | 2    |
        | a downgrade across several unchanged levels | 29      | 24     | 2    |

    Scenario: A metadata.version downgrade without a downgrade type is refused
      Given the finalized feature levels are metadata.version=25
      And the requested feature rows are metadata.version:24:1
      When the local controller plans the validate-only feature request
      Then feature metadata.version level 24 is refused for want of a downgrade type

    Scenario Outline: A downgrade that leaves a metadata change is refused
      Given the finalized feature levels are metadata.version=<current>
      And the requested feature rows are metadata.version:<target>:<kind>
      When the local controller plans the validate-only feature request
      Then the metadata.version downgrade from <current> to <target> is refused because <reason>

      Examples:
        | case                                    | current | target | kind | reason       |
        | a safe downgrade past a changed level   | 25      | 22     | 2    | Refusing to perform the requested downgrade because it might delete metadata information. |
        | leaving 3.6-IV1, which changed metadata | 13      | 12     | 2    | Refusing to perform the requested downgrade because it might delete metadata information. |
        | leaving 4.3-IV0, which changed metadata | 30      | 29     | 2    | Refusing to perform the requested downgrade because it might delete metadata information. |
        | an unsafe downgrade past a changed level | 17      | 15     | 3    | Unsafe metadata downgrade is not supported in this version. |

  Rule: metadata.version is finalized only inside the local range, which reaches past the latest stable level only with unstable.feature.versions.enable

    Scenario: A downgrade below the oldest supported metadata.version is refused
      Given the finalized feature levels are metadata.version=25
      And the requested feature rows are metadata.version:6:2
      When the local controller plans the validate-only feature request
      Then feature metadata.version level 6 is refused because Local controller 1 only supports versions 7-33

    Scenario: The latest stable metadata.version is finalized without the opt-in
      Given the finalized feature levels are metadata.version=30
      And the requested feature rows are metadata.version:33:1
      When the local controller plans the validate-only feature request
      Then the complete feature plan writes metadata.version=33 reports features metadata.version upgrades kraft to none and enables ELR false

    Scenario: The first unstable metadata.version is refused without the opt-in
      Given the finalized feature levels are metadata.version=30
      And the requested feature rows are metadata.version:34:1
      When the local controller plans the validate-only feature request
      Then feature metadata.version level 34 is refused because Local controller 1 only supports versions 7-33

    Scenario: The opt-in admits the first unstable metadata.version
      Given the finalized feature levels are metadata.version=30
      And unstable.feature.versions.enable is set on the local controller
      And the requested feature rows are metadata.version:34:1
      When the local controller plans the validate-only feature request
      Then the complete feature plan writes metadata.version=34 reports features metadata.version upgrades kraft to none and enables ELR false

  Rule: krabka.version follows the finalization rules of Kafka's other non-metadata features

    Scenario Outline: Krabka feature levels use the same non-metadata finalization rules
      Given the finalized feature levels are <levels>
      And the requested feature rows are krabka.version:<level>:<kind>
      When the local controller plans the validate-only feature request
      Then the complete feature plan writes krabka.version=<level> reports features krabka.version upgrades kraft to none and enables ELR false
      Examples:
        | case                               | levels                               | level | kind |
        | the first finalization             | metadata.version=30                  | 1     | 1    |
        | finalizing the current level again | metadata.version=30,krabka.version=1 | 1     | 1    |
        | a safe downgrade                   | metadata.version=30,krabka.version=1 | 0     | 2    |
        | an unsafe downgrade                | metadata.version=30,krabka.version=1 | 0     | 3    |

    Scenario Outline: Local support and explicit downgrade intent bound Krabka feature changes
      Given the finalized feature levels are <levels>
      And the requested feature rows are krabka.version:<level>:<kind>
      When the local controller plans the validate-only feature request
      Then feature krabka.version level <level> is refused because <reason>
      Examples:
        | case                                 | levels                               | level | kind | reason                                                                                                                |
        | a level above the local range        | metadata.version=30                  | 4     | 1    | Local controller 1 only supports versions 0-3                                                                         |
        | a downgrade without a downgrade type | metadata.version=30,krabka.version=1 | 0     | 1    | Can't downgrade the version of this feature without setting the upgrade type to either safe or unsafe downgrade.     |
        | a downgrade to a newer level         | metadata.version=30                  | 1     | 2    | Can't downgrade to a newer version.                                                                                  |

  Rule: Every registered broker must support a proposed krabka.version level

    Scenario Outline: Registered brokers must support the proposed Krabka feature level
      Given the finalized feature levels are <levels>
      And registered broker 2 advertises krabka.version range <range>
      And the requested feature rows are krabka.version:<level>:<kind>
      When the local controller plans the validate-only feature request
      Then the complete feature plan writes krabka.version=<level> reports features krabka.version upgrades kraft to none and enables ELR false
      Examples:
        | case                                  | levels                               | range | level | kind |
        | every broker supports the new level   | metadata.version=30                  | 0-1   | 1     | 1    |
        | every broker supports the lower level | metadata.version=30,krabka.version=1 | 0-0   | 0     | 2    |

    Scenario Outline: Missing or narrow registered broker support refuses the Krabka feature
      Given the finalized feature levels are metadata.version=30
      And registered broker 2 advertises krabka.version range <range>
      And the requested feature rows are krabka.version:1:1
      When the local controller plans the validate-only feature request
      Then feature krabka.version level 1 is refused because Broker 2 does not support this feature.
      Examples:
        | case                                            | range   |
        | a broker range that stops below the level       | 0-0     |
        | a broker that does not advertise krabka.version | missing |

  Rule: Enabling ELR establishes the cluster minimum ISR and clears every per-broker override

    Scenario Outline: Enabling ELR establishes cluster minimum ISR and clears all per-broker overrides
      Given ELR is disabled with cluster minimum ISR <cluster> broker <broker> minimum ISR <value> and static minimum ISR <static_value>
      When enabling ELR generates its minimum ISR safety records
      Then the ordered minimum ISR safety records are <records>
      Examples:
        | case                                              | cluster | broker | value | static_value | records          |
        | no cluster value takes the static minimum         | unset   | 1      | unset | 2            | cluster=2        |
        | a cluster value is kept                           | 3       | 1      | unset | 2            | none             |
        | an override is cleared and the static minimum set | unset   | 1      | 2     | 1            | cluster=1,1=null |
        | an override is cleared beside a cluster value     | 3       | 1      | 2     | 1            | 1=null           |
        | another broker override is cleared                | 3       | 7      | 2     | 1            | 7=null           |
