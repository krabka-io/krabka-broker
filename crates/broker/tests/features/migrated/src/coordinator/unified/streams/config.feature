Feature: Streams group defaults and Kafka override parsing
  Scenario: No group overrides retain all Kafka GA defaults
    Given no Streams group configuration values are overridden
    When the coordinator applies Streams group overrides to its default configuration
    Then every Streams setting equals the Kafka GA default
  Scenario Outline: Rack awareness tags are trimmed and reported in order
    Given the Streams rack awareness tags setting is JSON text <value>
    When the coordinator applies Streams group overrides to its default configuration
    Then rack tags become <tags> and DescribeConfigs reports JSON text <reported>
    Examples:
      | value | tags | reported |
      | "" | none | "" |
      | "  " | none | "" |
      | "zone" | zone | "zone" |
      | " zone ,\\track " | zone,rack | "zone,rack" |
  Scenario Outline: Rack awareness refuses duplicate and empty entries
    Given the Streams rack awareness tags setting is JSON text <value>
    When the coordinator applies Streams group overrides to its default configuration
    Then rack awareness tags are rejected with Kafka <reason> validation message
    Examples:
      | value | reason |
      | "rack,zone,rack" | duplicate |
      | "zone, zone" | duplicate |
      | "zone,,rack" | empty |
      | "zone," | empty |
  Scenario Outline: Share auto offset reset accepts only Kafka supported strategies and durations
    Given the Streams Share auto offset reset setting is JSON text <value>
    When the coordinator applies Streams group overrides to its default configuration
    Then auto offset reset selects <strategy> with duration seconds <seconds> and nanoseconds <nanos>
    Examples:
      | value | strategy | seconds | nanos |
      | "latest" | latest | 0 | 0 |
      | "earliest" | earliest | 0 | 0 |
      | "by_duration:PT1H" | duration | 3600 | 0 |
      | "by_duration:P1DT2H3M4.5S" | duration | 93784 | 500000000 |
      | "by_duration:PT0S" | duration | 0 | 0 |
      | "by_duration:-PT1H" | rejected | 0 | 0 |
      | "by_duration:PT-1H" | rejected | 0 | 0 |
      | "by_duration:PT" | rejected | 0 | 0 |
      | "by_duration:P" | rejected | 0 | 0 |
      | "by_duration:" | rejected | 0 | 0 |
      | "by_duration" | rejected | 0 | 0 |
      | "by_duration:1H" | rejected | 0 | 0 |
      | "by_duration:PT1X" | rejected | 0 | 0 |
      | "by_duration:P1W" | rejected | 0 | 0 |
      | "by_duration:PT1M1H" | rejected | 0 | 0 |
      | "Earliest" | rejected | 0 | 0 |
      | "none" | rejected | 0 | 0 |
      | "" | rejected | 0 | 0 |
