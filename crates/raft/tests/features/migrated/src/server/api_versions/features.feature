Feature: Kafka supported feature minimum and maximum levels
  Scenario Outline: Feature advertisement follows the protocol version and unstable feature policy
    Given unstable feature versions are <unstable>
    When supported features are advertised at ApiVersions version <version>
    Then metadata version spans its production minimum through <maximum> and modern feature minimums are included <modern> with share maximum <share>
    Examples:
      | unstable | version | maximum | modern | share |
      | false | 0 | stable | false | 1 |
      | false | 3 | stable | false | 1 |
      | false | 4 | stable | true | 1 |
      | false | 5 | stable | true | 1 |
      | true | 0 | trunk | false | 2 |
      | true | 3 | trunk | false | 2 |
      | true | 4 | trunk | true | 2 |
      | true | 5 | trunk | true | 2 |
