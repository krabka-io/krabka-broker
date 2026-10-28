Feature: Retaining producer batch bytes unless topic policy requires recompression
  Scenario: Producer LZ4 compression survives the verbatim append and Fetch
    Given a small LZ4 producer batch represents two hundred 512-byte Z records
    When the producer batch is appended to the topic
    Then the producer batch is accepted at offset zero
    When the stored producer batch is fetched
    Then LZ4 compression and every large record value are preserved

  Scenario: An uncompressed producer batch retains its CRC and record bytes
    Given an uncompressed producer batch contains three payload records
    When the producer batch is appended to the topic
    Then the producer batch is accepted at offset zero
    When the stored producer batch is fetched
    Then the record body CRC-covered header and CRC bytes are unchanged

  Scenario: A topic codec override recompresses producer records without data loss
    Given a zstd topic receives an LZ4 producer batch containing ten 256-byte Q records
    When the producer batch is appended to the topic
    Then the producer batch is accepted at offset zero
    When the stored producer batch is fetched
    Then the topic's zstd compression replaces LZ4 without changing record values
