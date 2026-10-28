Feature: Kafka fetch visibility
  Consumers see only committed and deliverable prefixes. Followers replicate the
  entire local log, while response watermarks still describe the committed state.

  Scenario: Consumer visibility respects retention, isolation and delivery (1: boundary read uncommitted consumer, start 2)
    Given fetch watermarks start 2 high 8 stable 6 end 10 deliverable 8
    When a consumer using read-uncommitted fetches offset 3
    Then the fetch offset is in-range and the computed window is nonempty
    And its visible prefix ends at 8 with stable offset 6 and abort metadata omitted
    And fetch reports high watermark 8 and stable offset 6

  Scenario: Consumer visibility respects retention, isolation and delivery (2: boundary read committed consumer, start 2)
    Given fetch watermarks start 2 high 8 stable 6 end 10 deliverable 8
    When a consumer using read-committed fetches offset 3
    Then the fetch offset is in-range and the computed window is nonempty
    And its visible prefix ends at 6 with stable offset 6 and abort metadata included
    And fetch reports high watermark 8 and stable offset 6

  Scenario: Consumer visibility respects retention, isolation and delivery (3: boundary below log start, start 2)
    Given fetch watermarks start 2 high 8 stable 6 end 10 deliverable 8
    When a consumer using read-uncommitted fetches offset 1
    Then the fetch offset is out-of-range and the computed window is nonempty
    And its visible prefix ends at 8 with stable offset 6 and abort metadata omitted
    And fetch reports high watermark 8 and stable offset 6

  Scenario: Consumer visibility respects retention, isolation and delivery (4: boundary lso capped at high watermark, start 2)
    Given fetch watermarks start 2 high 8 stable 9 end 10 deliverable 8
    When a consumer using read-committed fetches offset 3
    Then the fetch offset is in-range and the computed window is nonempty
    And its visible prefix ends at 8 with stable offset 8 and abort metadata included
    And fetch reports high watermark 8 and stable offset 8

  Scenario: Consumer visibility respects retention, isolation and delivery (5: boundary consumer caps before undelivered batch, start 2)
    Given fetch watermarks start 2 high 8 stable 6 end 10 deliverable 5
    When a consumer using read-uncommitted fetches offset 3
    Then the fetch offset is in-range and the computed window is nonempty
    And its visible prefix ends at 5 with stable offset 6 and abort metadata omitted
    And fetch reports high watermark 8 and stable offset 6

  Scenario: Consumer visibility respects retention, isolation and delivery (6: boundary consumer at delivery watermark is empty, start 2)
    Given fetch watermarks start 2 high 8 stable 6 end 10 deliverable 5
    When a consumer using read-uncommitted fetches offset 5
    Then the fetch offset is in-range and the computed window is empty
    And its visible prefix ends at 5 with stable offset 6 and abort metadata omitted
    And fetch reports high watermark 8 and stable offset 6

  Scenario: Consumer visibility respects retention, isolation and delivery (7: boundary read committed uses lowest delivery bound, start 2)
    Given fetch watermarks start 2 high 8 stable 6 end 10 deliverable 4
    When a consumer using read-committed fetches offset 3
    Then the fetch offset is in-range and the computed window is nonempty
    And its visible prefix ends at 4 with stable offset 6 and abort metadata included
    And fetch reports high watermark 8 and stable offset 6

  Scenario: Consumer visibility respects retention, isolation and delivery (8: boundary delivery above high watermark is capped, start 2)
    Given fetch watermarks start 2 high 8 stable 6 end 10 deliverable 10
    When a consumer using read-uncommitted fetches offset 8
    Then the fetch offset is in-range and the computed window is empty
    And its visible prefix ends at 8 with stable offset 6 and abort metadata omitted
    And fetch reports high watermark 8 and stable offset 6

  Scenario: Consumer visibility respects retention, isolation and delivery (9: boundary delivery below log start leaves empty window, start 2)
    Given fetch watermarks start 2 high 8 stable 6 end 10 deliverable 0
    When a consumer using read-committed fetches offset 2
    Then the fetch offset is in-range and the computed window is empty
    And its visible prefix ends at 0 with stable offset 6 and abort metadata included
    And fetch reports high watermark 8 and stable offset 6

  Scenario: Consumer visibility respects retention, isolation and delivery (10: boundary consumer uncommitted below log start, start 2)
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a consumer using read-uncommitted fetches offset 1
    Then the fetch offset is out-of-range and the computed window is nonempty
    And its visible prefix ends at 8 with stable offset 7 and abort metadata omitted
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Consumer visibility respects retention, isolation and delivery (11: boundary consumer uncommitted at log start, start 2)
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a consumer using read-uncommitted fetches offset 2
    Then the fetch offset is in-range and the computed window is nonempty
    And its visible prefix ends at 8 with stable offset 7 and abort metadata omitted
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Consumer visibility respects retention, isolation and delivery (12: boundary consumer uncommitted between high watermark and log end, start 2)
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a consumer using read-uncommitted fetches offset 9
    Then the fetch offset is in-range and the computed window is empty
    And its visible prefix ends at 8 with stable offset 7 and abort metadata omitted
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Consumer visibility respects retention, isolation and delivery (13: boundary consumer uncommitted at log end, start 2)
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a consumer using read-uncommitted fetches offset 10
    Then the fetch offset is in-range and the computed window is empty
    And its visible prefix ends at 8 with stable offset 7 and abort metadata omitted
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Consumer visibility respects retention, isolation and delivery (14: boundary consumer uncommitted beyond log end, start 2)
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a consumer using read-uncommitted fetches offset 11
    Then the fetch offset is out-of-range and the computed window is nonempty
    And its visible prefix ends at 8 with stable offset 7 and abort metadata omitted
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Consumer visibility respects retention, isolation and delivery (15: boundary consumer uncommitted maximum offset, start 2)
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a consumer using read-uncommitted fetches offset 9223372036854775807
    Then the fetch offset is out-of-range and the computed window is nonempty
    And its visible prefix ends at 8 with stable offset 7 and abort metadata omitted
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Consumer visibility respects retention, isolation and delivery (16: boundary consumer read committed below log start, start 2)
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a consumer using read-committed fetches offset 1
    Then the fetch offset is out-of-range and the computed window is nonempty
    And its visible prefix ends at 7 with stable offset 7 and abort metadata included
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Consumer visibility respects retention, isolation and delivery (17: boundary consumer read committed at log start, start 2)
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a consumer using read-committed fetches offset 2
    Then the fetch offset is in-range and the computed window is nonempty
    And its visible prefix ends at 7 with stable offset 7 and abort metadata included
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Consumer visibility respects retention, isolation and delivery (18: boundary consumer read committed between high watermark and log end, start 2)
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a consumer using read-committed fetches offset 9
    Then the fetch offset is in-range and the computed window is empty
    And its visible prefix ends at 7 with stable offset 7 and abort metadata included
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Consumer visibility respects retention, isolation and delivery (19: boundary consumer read committed at log end, start 2)
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a consumer using read-committed fetches offset 10
    Then the fetch offset is in-range and the computed window is empty
    And its visible prefix ends at 7 with stable offset 7 and abort metadata included
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Consumer visibility respects retention, isolation and delivery (20: boundary consumer read committed beyond log end, start 2)
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a consumer using read-committed fetches offset 11
    Then the fetch offset is out-of-range and the computed window is nonempty
    And its visible prefix ends at 7 with stable offset 7 and abort metadata included
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Consumer visibility respects retention, isolation and delivery (21: boundary consumer read committed maximum offset, start 2)
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a consumer using read-committed fetches offset 9223372036854775807
    Then the fetch offset is out-of-range and the computed window is nonempty
    And its visible prefix ends at 7 with stable offset 7 and abort metadata included
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Follower visibility reaches the log end regardless of consumer isolation (1: boundary follower reads to log end, start 2)
    Given fetch watermarks start 2 high 8 stable 6 end 10 deliverable 8
    When a follower using read-uncommitted fetches offset 8
    Then the fetch offset is in-range and the computed window is nonempty
    And its visible prefix ends at 10 with stable offset 6 and abort metadata omitted
    And fetch reports high watermark 8 and stable offset 6

  Scenario: Follower visibility reaches the log end regardless of consumer isolation (2: boundary caught up follower, start 2)
    Given fetch watermarks start 2 high 8 stable 6 end 10 deliverable 8
    When a follower using read-uncommitted fetches offset 10
    Then the fetch offset is in-range and the computed window is empty
    And its visible prefix ends at 10 with stable offset 6 and abort metadata omitted
    And fetch reports high watermark 8 and stable offset 6

  Scenario: Follower visibility reaches the log end regardless of consumer isolation (3: boundary follower ignores delivery watermark, start 2)
    Given fetch watermarks start 2 high 8 stable 6 end 10 deliverable 2
    When a follower using read-uncommitted fetches offset 3
    Then the fetch offset is in-range and the computed window is nonempty
    And its visible prefix ends at 10 with stable offset 6 and abort metadata omitted
    And fetch reports high watermark 8 and stable offset 6

  Scenario: Follower visibility reaches the log end regardless of consumer isolation (4: boundary follower uncommitted below log start, start 2)
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower using read-uncommitted fetches offset 1
    Then the fetch offset is out-of-range and the computed window is nonempty
    And its visible prefix ends at 10 with stable offset 7 and abort metadata omitted
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Follower visibility reaches the log end regardless of consumer isolation (5: boundary follower uncommitted at log start, start 2)
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower using read-uncommitted fetches offset 2
    Then the fetch offset is in-range and the computed window is nonempty
    And its visible prefix ends at 10 with stable offset 7 and abort metadata omitted
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Follower visibility reaches the log end regardless of consumer isolation (6: boundary follower uncommitted before log end, start 2)
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower using read-uncommitted fetches offset 9
    Then the fetch offset is in-range and the computed window is nonempty
    And its visible prefix ends at 10 with stable offset 7 and abort metadata omitted
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Follower visibility reaches the log end regardless of consumer isolation (7: boundary follower uncommitted at log end, start 2)
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower using read-uncommitted fetches offset 10
    Then the fetch offset is in-range and the computed window is empty
    And its visible prefix ends at 10 with stable offset 7 and abort metadata omitted
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Follower visibility reaches the log end regardless of consumer isolation (8: boundary follower uncommitted beyond log end, start 2)
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower using read-uncommitted fetches offset 11
    Then the fetch offset is out-of-range and the computed window is nonempty
    And its visible prefix ends at 10 with stable offset 7 and abort metadata omitted
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Follower visibility reaches the log end regardless of consumer isolation (9: boundary follower uncommitted maximum offset, start 2)
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower using read-uncommitted fetches offset 9223372036854775807
    Then the fetch offset is out-of-range and the computed window is nonempty
    And its visible prefix ends at 10 with stable offset 7 and abort metadata omitted
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Follower visibility reaches the log end regardless of consumer isolation (10: boundary follower read committed below log start, start 2)
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower using read-committed fetches offset 1
    Then the fetch offset is out-of-range and the computed window is nonempty
    And its visible prefix ends at 10 with stable offset 7 and abort metadata omitted
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Follower visibility reaches the log end regardless of consumer isolation (11: boundary follower read committed at log start, start 2)
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower using read-committed fetches offset 2
    Then the fetch offset is in-range and the computed window is nonempty
    And its visible prefix ends at 10 with stable offset 7 and abort metadata omitted
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Follower visibility reaches the log end regardless of consumer isolation (12: boundary follower read committed before log end, start 2)
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower using read-committed fetches offset 9
    Then the fetch offset is in-range and the computed window is nonempty
    And its visible prefix ends at 10 with stable offset 7 and abort metadata omitted
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Follower visibility reaches the log end regardless of consumer isolation (13: boundary follower read committed at log end, start 2)
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower using read-committed fetches offset 10
    Then the fetch offset is in-range and the computed window is empty
    And its visible prefix ends at 10 with stable offset 7 and abort metadata omitted
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Follower visibility reaches the log end regardless of consumer isolation (14: boundary follower read committed beyond log end, start 2)
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower using read-committed fetches offset 11
    Then the fetch offset is out-of-range and the computed window is nonempty
    And its visible prefix ends at 10 with stable offset 7 and abort metadata omitted
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Follower visibility reaches the log end regardless of consumer isolation (15: boundary follower read committed maximum offset, start 2)
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower using read-committed fetches offset 9223372036854775807
    Then the fetch offset is out-of-range and the computed window is nonempty
    And its visible prefix ends at 10 with stable offset 7 and abort metadata omitted
    And fetch reports high watermark 8 and stable offset 7
