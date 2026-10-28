Feature: Fetch minimum byte floor

  Scenario: a fetch below its min bytes is held until max wait and then answers
    Given a broker topic fetch-min-bytes containing the first small record
    When Fetch requests at least 65536 bytes for 2000 ms while a second small record arrives after 300 ms
    Then the answer waits at least 1800 ms and returns error 0, watermark 2, and batch offsets 0,1
