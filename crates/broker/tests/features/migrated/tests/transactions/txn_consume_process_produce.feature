Feature: Consume-process-produce output commits with the input offset
  Scenario: send offsets to transaction atomic with records
    Given input contains i0 through i4 and transactional output is empty
    When group cpp-g transforms its input records into output within one open transaction
    Then the processing transaction has consumed all five input records
    When the producer enrolls the input offset and commits the output transaction
    And a read_committed verifier polls output for the five transformed records
    Then all five output records are visible as committed data
