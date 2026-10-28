Feature: Streaming metadata ranges
  Scenario: A moved leader resumes at the first undelivered offset
    Given leader 1 serves offsets 0 and 1 then moves to leader 2 serving offsets 2 and 3
    When the requested range is read
    Then offsets 0 through 3 are delivered once and fetching resumes from 2 on leader 2

  Scenario: Leader discovery retries until a leader exists
    Given the partition initially has no leader then broker 3 serves offsets 0 and 1
    When the requested range is read
    Then offsets 0 and 1 are delivered after two leader lookups and one fetch from broker 3

  Scenario: Fatal fetch and lookup errors retain their causes
    Given the first range fetch fails with corrupt message and the next leader lookup is unauthorized
    When the requested range is read twice
    Then both reads stop with their precise fetch and metadata authorization errors

  Scenario: A page without batch progress fails the range
    Given a range from 4 to 10 receives an empty page with no next offset
    When the requested range is read
    Then the read reports that no batch was returned at 4 before 10

  Scenario: Visitor closure stops delivery immediately
    Given a range from 0 to 6 has a page containing offsets 0 through 2
    When the range visitor closes on its first record
    Then only offset 0 reaches the visitor and the closed error stops further fetches

  Scenario: An empty range performs no I O
    Given an empty range from 5 to 5
    When the requested range is read
    Then no records are delivered and no network requests are made

  Scenario Outline: An invalid partition performs no I O
    Given a range from 0 to 1 targets partition <partition> of a two-partition topic
    When the requested range is read
    Then the partition is rejected with count 2 without network requests
    Examples:
      | partition |
      | -1        |
      | 2         |
