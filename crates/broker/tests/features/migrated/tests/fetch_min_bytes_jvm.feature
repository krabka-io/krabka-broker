Feature: Kafka agreement on the Fetch minimum byte floor

  Scenario: krabka and kafka both hold a fetch below its min bytes
    Given Kafka 4.3.1 and krabka each have topic fetch-min-bytes containing the first small record
    When each Fetch requests at least 65536 bytes for 2000 ms while another small record arrives after 300 ms
    Then both answers wait at least 1800 ms and return identical errors, watermarks, and batch offsets
