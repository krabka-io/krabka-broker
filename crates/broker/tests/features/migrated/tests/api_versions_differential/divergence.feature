Feature: Explaining the advertised API differences from Kafka

  Scenario: tests: join covers both tables and labels each key
    Given oracle image oracle:1.2.3 and sample Fetch-range and Produce-served-range explanations
    And krabka advertises:
      | key | minimum | maximum |
      | 3   | 0       | 13      |
      | 1   | 4       | 17      |
      | 80  | 0       | 1       |
    And Kafka advertises:
      | key | minimum | maximum |
      | 3   | 0       | 13      |
      | 1   | 4       | 18      |
      | 88  | 0       | 0       |
    When the advertised tables are joined into a divergence report
    Then the report has exactly these API rows in key order:
      | key | name                  | krabka_minimum | krabka_maximum | kafka_minimum | kafka_maximum | verdict       | reason                      |
      | 1   | Fetch                 | 4              | 17             | 4             | 18            | range-differs | Fetch serves older versions. |
      | 3   | Metadata              | 0              | 13             | 0             | 13            | same          | none                        |
      | 80  | AddRaftVoter          | 0              | 1              | absent        | absent        | krabka-only   | none                        |
      | 88  | StreamsGroupHeartbeat | absent         | absent         | 0             | 0             | kafka-only    | none                        |

  Scenario: tests: every recorded intent is non empty and keyed once
    Given the checked-in API divergence explanations
    When all configured divergence explanations are enumerated
    Then every explanation has a unique API key and a nonempty reason

  Scenario: tests: a served range divergence is recorded on a matching advertised range
    Given oracle image oracle:1.2.3 and sample Fetch-range and Produce-served-range explanations
    And krabka advertises:
      | key | minimum | maximum |
      | 0   | 0       | 13      |
    And Kafka advertises:
      | key | minimum | maximum |
      | 0   | 0       | 13      |
    When the advertised tables are joined into a divergence report
    Then the report has exactly these API rows in key order:
      | key | name    | krabka_minimum | krabka_maximum | kafka_minimum | kafka_maximum | verdict       | reason                       |
      | 0   | Produce | 0              | 13             | 0             | 13            | range-differs | Produce serves older versions. |

  Scenario: tests: a served range divergence whose advertised range moves panics
    Given oracle image oracle:1.2.3 and sample Fetch-range and Produce-served-range explanations
    And krabka advertises:
      | key | minimum | maximum |
      | 0   | 3       | 13      |
    And Kafka advertises:
      | key | minimum | maximum |
      | 0   | 0       | 13      |
    When the advertised tables are joined into a divergence report
    Then the report is rejected because no longer does

  Scenario: tests: an unrecorded range divergence panics
    Given oracle image oracle:1.2.3 and sample Fetch-range and Produce-served-range explanations
    And krabka advertises:
      | key | minimum | maximum |
      | 3   | 0       | 13      |
    And Kafka advertises:
      | key | minimum | maximum |
      | 3   | 0       | 12      |
    When the advertised tables are joined into a divergence report
    Then the report is rejected because no intent is recorded

  Scenario: tests: the recorded tables admit no range divergence
    Given the checked-in API divergence explanations
    And krabka advertises:
      | key | minimum | maximum |
      | 1   | 0       | 18      |
    And Kafka advertises:
      | key | minimum | maximum |
      | 1   | 4       | 18      |
    When the advertised tables are joined into a divergence report
    Then the report is rejected because no intent is recorded

  Scenario: tests: the checked in report is exactly what store writes
    Given the checked-in divergence report loaded from a temporary file
    When the report is stored in a fresh nested JSON path and reloaded
    Then the stored bytes exactly match the checked-in report

  Scenario: tests: report round trips through json
    Given oracle image oracle:1.2.3 and sample Fetch-range and Produce-served-range explanations
    And krabka advertises:
      | key | minimum | maximum |
      | 1   | 0       | 18      |
    And Kafka advertises:
      | key | minimum | maximum |
      | 1   | 4       | 18      |
    When the advertised tables are joined into a divergence report
    And the report is stored in a fresh nested JSON path and reloaded
    Then the reloaded report equals the original report

  Scenario: the public builder reproduces the reviewed divergence from its advertised ranges
    Given the checked-in divergence report loaded from a temporary file
    When the public report builder rejoins the recorded advertised ranges
    Then the rebuilt report equals the complete reviewed report
