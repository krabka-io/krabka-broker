Feature: Official Kafka formatters decode persisted consumer coordinator records
  Scenario: kafka formatters decode krabka consumer offsets
    Given topic krabka-offsets-formatter contains record-0 through record-4 for classic and KIP-848 consumers
    When classic group krabka-offsets-formatter-classic and KIP-848 group krabka-offsets-formatter-next-gen consume and close
    Then both groups commit position 5
    When Kafka 4.3.1 group-metadata and offsets formatters decode the complete consumer-offsets log
    Then the latest decoded offset rows contain exactly these commit fields
      | key | group                                  | topic                    | partition | version | offset | metadata |
      | 1   | krabka-offsets-formatter-classic        | krabka-offsets-formatter  | 0         | 4       | 5      |          |
      | 1   | krabka-offsets-formatter-next-gen       | krabka-offsets-formatter  | 0         | 4       | 5      |          |
    And classic key 2 value 3 metadata has consumer protocol range, its dynamic member as leader, and these member fields
      | client                          | session | rebalance |
      | krabka-offsets-formatter-client  | 10000   | 60000     |
    And KIP-848 has no classic metadata and offsets decoding skips at least 3 additional KIP-848 records
