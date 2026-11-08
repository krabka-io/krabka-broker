Feature: Kafka supported feature minimum and maximum levels
  Scenario Outline: Feature advertisement follows the protocol version and unstable feature policy
    Given unstable feature versions are <unstable>
    When supported features are advertised at ApiVersions version <version>
    Then metadata version spans its production minimum through <maximum> and modern feature minimums are included <modern> with share maximum <share>
    Examples:
      | case                                                         | unstable | version | maximum | modern | share |
      | stable levels at version 0                                   | false | 0 | stable | false | 2 |
      | stable levels at version 3, the last without modern minimums | false | 3 | stable | false | 2 |
      | stable levels at version 4, the first with modern minimums   | false | 4 | stable | true | 2 |
      | stable levels at version 5                                   | false | 5 | stable | true | 2 |
      | trunk levels at version 0                                    | true | 0 | trunk | false | 2 |
      | trunk levels at version 3, the last without modern minimums  | true | 3 | trunk | false | 2 |
      | trunk levels at version 4, the first with modern minimums    | true | 4 | trunk | true | 2 |
      | trunk levels at version 5                                    | true | 5 | trunk | true | 2 |
