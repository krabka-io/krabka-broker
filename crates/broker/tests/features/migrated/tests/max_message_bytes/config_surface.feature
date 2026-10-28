Feature: Operators configure the maximum message size through topic configuration
  Scenario: A created message cap is visible as a mutable integer topic override
    Given orders has a maximum message size of 2048 bytes
    When the operator describes the maximum message size
    Then the complete config entry reports a mutable dynamic topic integer of 2048

  Scenario: Raising a live cap admits the previously refused batch
    Given orders has a maximum message size of 2048 bytes
    When a producer submits a 2049 byte batch
    Then the complete produce response refuses the oversized batch
    When the operator incrementally raises the cap to 2049 bytes
    Then the alteration succeeds without an error message
    When the operator describes the maximum message size
    Then the described maximum message size is 2049 bytes
    When a producer submits a 2049 byte batch
    Then the same batch is accepted at offset zero

  Scenario: Alter configuration applies Kafka integer boundary validation
    Given orders uses the broker's default maximum message size
    When the operator tries the maximum message size values
      | value |
      | 0 |
      | 2147483647 |
      | -1 |
      | 2147483648 |
      | plenty |
    Then integer boundary values have the expected errors and refusal messages
      | value | code |
      | 0 | 0 |
      | 2147483647 | 0 |
      | -1 | 40 |
      | 2147483648 | 40 |
      | plenty | 40 |
