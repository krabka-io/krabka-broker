Feature: Multi-request broker workflows through the Kafka client protocol
  Scenario: Timestamp lookups return the first matching record and Kafka local sentinels
    Given the local topic contains these timestamped records
      | value | timestamp |
      | a     | 100       |
      | b     | 200       |
      | c     | 300       |
    When local offsets are queried for timestamps 150,-4,-3
    Then timestamp query 150 returns offset 1 and record timestamp 200
    And the earliest local timestamp query returns offset zero
    And timestamp query -3 returns offset 2 and record timestamp 300

  Scenario: A negotiated client creates a topic produces records and fetches them
    Given an in-process broker serves ordinary client requests
    When the client negotiates supported API versions
    Then API version negotiation succeeds
    When the client creates topic e2e and requests topic metadata
    Then topic creation succeeds and e2e is visible in metadata
    When records a b and c are produced to e2e
    Then the produce is acknowledged successfully
    When the latest offset of e2e is requested
    Then the latest offset succeeds at three
    When the client fetches the e2e partition
    Then the fetch contains all three records

  Scenario: A one-way produce appends without a reply and leaves its connection usable
    Given topic one-way-produce exists for a raw protocol client
    When a Produce v9 frame sends one record with acks zero
    Then no response frame becomes readable for the unacknowledged produce
    When a Metadata v12 frame with correlation two reuses the same connection
    Then the connection returns the Metadata correlation id two
    When the one-way-produce log end is queried
    Then the unacknowledged record is present at log end one

  Scenario: A broker recovers persisted topic partitions when it rejoins its log directory
    Given a fresh broker log directory
    When a broker creates persisted with two partitions and shuts down
    Then the persisted topic creation was accepted
    When the broker rejoins using the same log directory and metadata is requested
    Then the recovered topic metadata still has two partitions
