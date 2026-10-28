Feature: Produce framing rejects ambiguous fields before checking record CRC
  Scenario Outline: Invalid verbatim records retain Kafka corruption and invalid-record distinctions
    Given a Produce records slice contains <shape>
    When Produce version 13 prepares that records slice without recompression
    Then the records slice is rejected with Kafka error <code>
    Examples:
      | shape                                     | code |
      | a CRC mismatch                            | 2    |
      | a CRC mismatch followed by a second batch | 87   |
      | two valid batches                         | 87   |
      | batch size 13                             | 2    |
      | batch size -1                             | 2    |
      | batch size 20 truncated to 32 bytes        | 2    |
      | magic 3                                   | 2    |
      | magic 255                                 | 2    |
      | one byte missing from the record body      | 87   |
      | only the first 11 bytes                    | 87   |
