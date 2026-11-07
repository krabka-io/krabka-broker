Feature: Fault drills reproduce a misbehaving object store

  A fault drill wraps an object store so that it throttles, fails, loses
  objects and stalls the way a real S3 bucket does during an incident. The
  broker's tiered-storage and diskless drills, in
  crates/broker/src/remote_log_manager/tests/store_faults.rs and
  crates/broker/src/diskless/flusher.rs, run the production paths against it
  and assert on which calls failed and how many reached the store.

  Rule: A failure rate is a fixed schedule of calls, the same on every drill

    Scenario Outline: A throttle rate picks the same puts on every drill
      Given the store answers <rate> in every thousand puts with SlowDown
      When a broker writes eight segments one after another
      Then the throttled puts are <throttled>

      Examples:
        | case                            | rate | throttled              |
        | no throttling                   | 0    | none                   |
        | too rare to reach eight puts    | 1    | none                   |
        | one put in four                 | 250  | 4, 8                   |
        | every second put                | 500  | 2, 4, 6, 8             |
        | every put                       | 1000 | 1, 2, 3, 4, 5, 6, 7, 8 |

  Rule: A fault touches only the operation it names

    Scenario: Throttled puts leave reads of archived segments working
      Given a segment has been archived
      And the store answers every put with SlowDown
      When a broker attempts to read the archived segment
      Then the read returns the archived segment

    Scenario: Throttled puts also throttle a large upload in parts
      Given the store answers every put with SlowDown
      When a broker attempts to upload a large segment file in parts
      Then the broker sees a backend failure naming SlowDown
      And the store holds no segment

    Scenario: A lost object on reads is also lost to a size lookup
      Given a segment has been archived
      And the store answers every read with NoSuchKey
      When a broker attempts to look up the size of the archived segment
      Then the broker sees the archived segment as not found

  Rule: A transport fault reaches the broker as a backend failure, a lost object as not found

    Scenario Outline: A transport fault on a write reaches the broker as a backend failure
      Given the store answers every put with <fault>
      When a broker attempts to write a segment
      Then the broker sees a backend failure naming <fault>
      And the store holds no segment

      Examples:
        | case              | fault         |
        | a throttled store | SlowDown      |
        | a failing store   | InternalError |

    Scenario: A lost object reaches the broker as not found
      Given a segment has been archived
      And the store answers every read with NoSuchKey
      When a broker attempts to read the archived segment
      Then the broker sees the archived segment as not found

  Rule: Every call that reaches the store is counted, failed ones included

    Scenario: Throttled puts count as attempts
      Given the store answers 500 in every thousand puts with SlowDown
      When a broker writes eight segments one after another
      Then the store has counted 8 put attempts

    Scenario: A failed listing counts as an attempt
      Given the store answers every listing with InternalError
      When a broker attempts to list the store
      Then the broker sees a backend failure naming InternalError
      And the store has counted 1 listing attempt

  Rule: A stalled call answers correctly, only late

    Scenario: A stalled listing returns every segment late
      Given a segment has been archived
      And the store holds every listing for 30 seconds
      When a broker attempts to list the store
      Then the listing returns the archived segment
      And the store answers 30 seconds late

    Scenario: A stalled put stores the segment late
      Given the store holds every put for 30 seconds
      When a broker attempts to write a segment
      Then the store holds the segment
      And the store answers 30 seconds late
