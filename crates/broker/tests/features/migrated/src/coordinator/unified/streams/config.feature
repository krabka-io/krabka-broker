Feature: Streams group defaults and Kafka override parsing

  Rule: A group with no overrides keeps Kafka's GA defaults

    Scenario: No group overrides retain all Kafka GA defaults
      Given no Streams group configuration values are overridden
      When the coordinator applies Streams group overrides to its default configuration
      Then every Streams setting equals the Kafka GA default

  Rule: Rack awareness tags are a trimmed list reported in order

    Scenario Outline: Rack awareness tags are trimmed and reported in order
      Given the Streams rack awareness tags setting is JSON text <value>
      When the coordinator applies Streams group overrides to its default configuration
      Then rack tags become <tags> and DescribeConfigs reports JSON text <reported>

      Examples:
        | case                       | value             | tags      | reported    |
        | an empty list              | ""                | none      | ""          |
        | blanks only                | "  "              | none      | ""          |
        | one tag                    | "zone"            | zone      | "zone"      |
        | tags trimmed of whitespace | " zone ,\\track " | zone,rack | "zone,rack" |

  Rule: Rack awareness refuses a repeated or an empty tag key

    Scenario Outline: Rack awareness refuses duplicate and empty entries
      Given the Streams rack awareness tags setting is JSON text <value>
      When the coordinator applies Streams group overrides to its default configuration
      Then rack awareness tags are rejected with Kafka <reason> validation message

      Examples:
        | case                           | value            | reason    |
        | a repeated tag                 | "rack,zone,rack" | duplicate |
        | a repeat that differs by blank | "zone, zone"     | duplicate |
        | an empty tag between two       | "zone,,rack"     | empty     |
        | a trailing empty tag           | "zone,"          | empty     |

  Rule: Share auto offset reset accepts only Kafka's strategies and non-negative durations

    Scenario Outline: A Kafka strategy or duration is accepted
      Given the Streams Share auto offset reset setting is JSON text <value>
      When the coordinator applies Streams group overrides to its default configuration
      Then auto offset reset selects <strategy> with duration seconds <seconds> and nanoseconds <nanos>

      Examples:
        | case                     | value                      | strategy | seconds | nanos     |
        | the latest offset        | "latest"                   | latest   | 0       | 0         |
        | the earliest offset      | "earliest"                 | earliest | 0       | 0         |
        | an hour                  | "by_duration:PT1H"         | duration | 3600    | 0         |
        | days and fractional time | "by_duration:P1DT2H3M4.5S" | duration | 93784   | 500000000 |
        | a zero duration          | "by_duration:PT0S"         | duration | 0       | 0         |

    Scenario Outline: A value that is no Kafka strategy is refused
      Given the Streams Share auto offset reset setting is JSON text <value>
      When the coordinator applies Streams group overrides to its default configuration
      Then the share auto offset reset is refused as no Kafka strategy

      Examples:
        | case                          | value                |
        | a negative duration           | "by_duration:-PT1H"  |
        | a negative component          | "by_duration:PT-1H"  |
        | an empty time section         | "by_duration:PT"     |
        | an empty period               | "by_duration:P"      |
        | an empty duration             | "by_duration:"       |
        | the prefix without a colon    | "by_duration"        |
        | a duration without its P      | "by_duration:1H"     |
        | an unknown time unit          | "by_duration:PT1X"   |
        | weeks, which Java refuses     | "by_duration:P1W"    |
        | components out of order       | "by_duration:PT1M1H" |
        | a strategy in the wrong case  | "Earliest"           |
        | the consumer strategy none    | "none"               |
        | an empty value                | ""                   |

  Rule: DescribeConfigs reports the strategy as Java's Duration.toString renders it

    Scenario: A group with no strategy reports the latest offset
      Given no Streams group configuration values are overridden
      When the coordinator applies Streams group overrides to its default configuration
      Then DescribeConfigs reports share.auto.offset.reset as latest

    Scenario Outline: A configured strategy is reported in Java's form and parses back to itself
      Given the Streams Share auto offset reset setting is JSON text <value>
      When the coordinator applies Streams group overrides to its default configuration
      Then DescribeConfigs reports share.auto.offset.reset as <reported>
      And the reported strategy parses back to the strategy the group runs with

      Examples:
        | case                          | value                      | reported                |
        | the earliest offset           | "earliest"                 | earliest                |
        | an hour                       | "by_duration:PT1H"         | by_duration:PT1H        |
        | days fold into hours          | "by_duration:P1DT2H3M4.5S" | by_duration:PT26H3M4.5S |
        | a zero duration keeps seconds | "by_duration:PT0S"         | by_duration:PT0S        |
        | lower case and minutes        | "by_duration:pt90m"        | by_duration:PT1H30M     |

  Rule: A stored override within the broker's bounds is applied and reported

    Scenario Outline: An override within bounds is applied and reported
      Given the group stores <setting> as <value>
      When the coordinator applies Streams group overrides to its default configuration
      Then the group runs with only <setting> changed, to <value>
      And DescribeConfigs reports <setting> as <value>

      Examples:
        | case                            | setting                            | value |
        | no initial rebalance delay      | streams.initial.rebalance.delay.ms | 0     |
        | a shorter assignment interval   | streams.assignment.interval.ms     | 250   |

  Rule: A stored override outside the broker's bounds is capped to the nearest bound

    Scenario Outline: An override past a bound runs at that bound
      Given the group stores <setting> as <value>
      When the coordinator applies Streams group overrides to its default configuration
      Then the group runs with only <setting> changed, to <capped>

      Examples:
        | case                                   | setting                        | value   | capped |
        | a session timeout below the minimum    | streams.session.timeout.ms     | 1000    | 45000  |
        | a session timeout above the maximum    | streams.session.timeout.ms     | 3600000 | 60000  |
        | a heartbeat interval of zero           | streams.heartbeat.interval.ms  | 0       | 5000   |
        | a negative heartbeat interval          | streams.heartbeat.interval.ms  | -1      | 5000   |
        | a heartbeat interval above the maximum | streams.heartbeat.interval.ms  | 60000   | 15000  |
        | an assignment interval above 15 s      | streams.assignment.interval.ms | 3600000 | 15000  |
        | more standby replicas than two         | streams.num.standby.replicas   | 5       | 2      |

  Rule: A stored value that is not a whole number of milliseconds is refused

    Scenario: A session timeout that is not a number is refused
      Given the group stores streams.session.timeout.ms as soon
      When the coordinator applies Streams group overrides to its default configuration
      Then the overrides are refused because streams.session.timeout.ms must be an integer

  Rule: A key of another coordinator leaves the Streams overrides intact

    Scenario: Consumer, share and dead letter queue keys beside a Streams override are ignored
      Given the group stores streams.num.standby.replicas as 1
      And the group stores keys that other coordinators own
      When the coordinator applies Streams group overrides to its default configuration
      Then the group runs with only streams.num.standby.replicas changed, to 1
