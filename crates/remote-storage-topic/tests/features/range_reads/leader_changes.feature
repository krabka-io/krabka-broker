Feature: Streaming metadata ranges across leader changes
  Scenario: A moved leader resumes at the first undelivered offset
    Given leader 1 serves offsets 0 and 1 then moves to leader 2 serving offsets 2 and 3
    When the requested range is read
    Then offsets 0 through 3 are delivered once and fetching resumes from 2 on leader 2

  Scenario: Leader discovery retries until a leader exists
    Given the partition initially has no leader then broker 3 serves offsets 0 and 1
    When the requested range is read
    Then offsets 0 and 1 are delivered after two leader lookups and one fetch from broker 3
