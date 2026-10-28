Feature: tests jvm acceptance sasl kafka 4 3 1 client

  Scenario: kafka 4 3 1 client authenticates with every mechanism
    Then jvm acceptance sasl kafka 4 3 1 client: kafka 4 3 1 client authenticates with every mechanism

  Scenario: kafka 4 3 1 client reports kafkas refusal for bad credentials
    Then jvm acceptance sasl kafka 4 3 1 client: kafka 4 3 1 client reports kafkas refusal for bad credentials
