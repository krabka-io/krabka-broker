Feature: Large plaintext Fetch preserves bytes and uses the native drain path

  Scenario: large message fetch round trips byte exact
    Given a plaintext topic big and 64 distinct records of 2048 bytes
    When the records are produced and fetched with byte budget 8388608 and wait 200 ms
    Then Produce and Fetch succeed and every record value matches its original bytes
    And exactly one response uses the platform plaintext drain path and no fallback path
