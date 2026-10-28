Feature: src network codec

  Scenario: tests: kafka max frame size is one hundred mib
    Then network codec tests: kafka max frame size is one hundred mib

  Scenario: tests: codec decodes frames larger than tokio default but within kafka max
    Then network codec tests: codec decodes frames larger than tokio default but within kafka max

  Scenario: tests: codec rejects frames over kafka max
    Then network codec tests: codec rejects frames over kafka max
