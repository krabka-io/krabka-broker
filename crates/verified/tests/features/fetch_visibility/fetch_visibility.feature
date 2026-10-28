Feature: Kafka fetch visibility

  Scenario: Fetch bounds and reported watermarks: read uncommitted consumer
    Given fetch watermarks start 2 high 8 stable 6 end 10 deliverable 8
    When a follower false committed false fetches offset 3
    Then fetch bounds are out of range false empty false limit 8 stable 6 aborts false
    And fetch reports high watermark 8 and stable offset 6

  Scenario: Fetch bounds and reported watermarks: read committed consumer
    Given fetch watermarks start 2 high 8 stable 6 end 10 deliverable 8
    When a follower false committed true fetches offset 3
    Then fetch bounds are out of range false empty false limit 6 stable 6 aborts true
    And fetch reports high watermark 8 and stable offset 6

  Scenario: Fetch bounds and reported watermarks: follower reads to log end
    Given fetch watermarks start 2 high 8 stable 6 end 10 deliverable 8
    When a follower true committed false fetches offset 8
    Then fetch bounds are out of range false empty false limit 10 stable 6 aborts false
    And fetch reports high watermark 8 and stable offset 6

  Scenario: Fetch bounds and reported watermarks: caught up follower
    Given fetch watermarks start 2 high 8 stable 6 end 10 deliverable 8
    When a follower true committed false fetches offset 10
    Then fetch bounds are out of range false empty true limit 10 stable 6 aborts false
    And fetch reports high watermark 8 and stable offset 6

  Scenario: Fetch bounds and reported watermarks: below log start
    Given fetch watermarks start 2 high 8 stable 6 end 10 deliverable 8
    When a follower false committed false fetches offset 1
    Then fetch bounds are out of range true empty false limit 8 stable 6 aborts false
    And fetch reports high watermark 8 and stable offset 6

  Scenario: Fetch bounds and reported watermarks: lso capped at high watermark
    Given fetch watermarks start 2 high 8 stable 9 end 10 deliverable 8
    When a follower false committed true fetches offset 3
    Then fetch bounds are out of range false empty false limit 8 stable 8 aborts true
    And fetch reports high watermark 8 and stable offset 8

  Scenario: Fetch bounds and reported watermarks: follower ignores delivery watermark
    Given fetch watermarks start 2 high 8 stable 6 end 10 deliverable 2
    When a follower true committed false fetches offset 3
    Then fetch bounds are out of range false empty false limit 10 stable 6 aborts false
    And fetch reports high watermark 8 and stable offset 6

  Scenario: Fetch bounds and reported watermarks: consumer caps before undelivered batch
    Given fetch watermarks start 2 high 8 stable 6 end 10 deliverable 5
    When a follower false committed false fetches offset 3
    Then fetch bounds are out of range false empty false limit 5 stable 6 aborts false
    And fetch reports high watermark 8 and stable offset 6

  Scenario: Fetch bounds and reported watermarks: consumer at delivery watermark is empty
    Given fetch watermarks start 2 high 8 stable 6 end 10 deliverable 5
    When a follower false committed false fetches offset 5
    Then fetch bounds are out of range false empty true limit 5 stable 6 aborts false
    And fetch reports high watermark 8 and stable offset 6

  Scenario: Fetch bounds and reported watermarks: read committed uses lowest delivery bound
    Given fetch watermarks start 2 high 8 stable 6 end 10 deliverable 4
    When a follower false committed true fetches offset 3
    Then fetch bounds are out of range false empty false limit 4 stable 6 aborts true
    And fetch reports high watermark 8 and stable offset 6

  Scenario: Fetch bounds and reported watermarks: delivery above high watermark is capped
    Given fetch watermarks start 2 high 8 stable 6 end 10 deliverable 10
    When a follower false committed false fetches offset 8
    Then fetch bounds are out of range false empty true limit 8 stable 6 aborts false
    And fetch reports high watermark 8 and stable offset 6

  Scenario: Fetch bounds and reported watermarks: delivery below log start leaves empty window
    Given fetch watermarks start 2 high 8 stable 6 end 10 deliverable 0
    When a follower false committed true fetches offset 2
    Then fetch bounds are out of range false empty true limit 0 stable 6 aborts true
    And fetch reports high watermark 8 and stable offset 6

  Scenario: Fetch bounds and reported watermarks: consumer uncommitted below log start
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower false committed false fetches offset 1
    Then fetch bounds are out of range true empty false limit 8 stable 7 aborts false
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Fetch bounds and reported watermarks: consumer uncommitted at log start
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower false committed false fetches offset 2
    Then fetch bounds are out of range false empty false limit 8 stable 7 aborts false
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Fetch bounds and reported watermarks: consumer uncommitted between high watermark and log end
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower false committed false fetches offset 9
    Then fetch bounds are out of range false empty true limit 8 stable 7 aborts false
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Fetch bounds and reported watermarks: consumer uncommitted at log end
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower false committed false fetches offset 10
    Then fetch bounds are out of range false empty true limit 8 stable 7 aborts false
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Fetch bounds and reported watermarks: consumer uncommitted beyond log end
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower false committed false fetches offset 11
    Then fetch bounds are out of range true empty false limit 8 stable 7 aborts false
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Fetch bounds and reported watermarks: consumer uncommitted maximum offset
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower false committed false fetches offset 9223372036854775807
    Then fetch bounds are out of range true empty false limit 8 stable 7 aborts false
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Fetch bounds and reported watermarks: consumer read committed below log start
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower false committed true fetches offset 1
    Then fetch bounds are out of range true empty false limit 7 stable 7 aborts true
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Fetch bounds and reported watermarks: consumer read committed at log start
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower false committed true fetches offset 2
    Then fetch bounds are out of range false empty false limit 7 stable 7 aborts true
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Fetch bounds and reported watermarks: consumer read committed between high watermark and log end
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower false committed true fetches offset 9
    Then fetch bounds are out of range false empty true limit 7 stable 7 aborts true
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Fetch bounds and reported watermarks: consumer read committed at log end
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower false committed true fetches offset 10
    Then fetch bounds are out of range false empty true limit 7 stable 7 aborts true
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Fetch bounds and reported watermarks: consumer read committed beyond log end
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower false committed true fetches offset 11
    Then fetch bounds are out of range true empty false limit 7 stable 7 aborts true
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Fetch bounds and reported watermarks: consumer read committed maximum offset
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower false committed true fetches offset 9223372036854775807
    Then fetch bounds are out of range true empty false limit 7 stable 7 aborts true
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Fetch bounds and reported watermarks: follower uncommitted below log start
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower true committed false fetches offset 1
    Then fetch bounds are out of range true empty false limit 10 stable 7 aborts false
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Fetch bounds and reported watermarks: follower uncommitted at log start
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower true committed false fetches offset 2
    Then fetch bounds are out of range false empty false limit 10 stable 7 aborts false
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Fetch bounds and reported watermarks: follower uncommitted before log end
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower true committed false fetches offset 9
    Then fetch bounds are out of range false empty false limit 10 stable 7 aborts false
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Fetch bounds and reported watermarks: follower uncommitted at log end
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower true committed false fetches offset 10
    Then fetch bounds are out of range false empty true limit 10 stable 7 aborts false
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Fetch bounds and reported watermarks: follower uncommitted beyond log end
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower true committed false fetches offset 11
    Then fetch bounds are out of range true empty false limit 10 stable 7 aborts false
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Fetch bounds and reported watermarks: follower uncommitted maximum offset
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower true committed false fetches offset 9223372036854775807
    Then fetch bounds are out of range true empty false limit 10 stable 7 aborts false
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Fetch bounds and reported watermarks: follower read committed below log start
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower true committed true fetches offset 1
    Then fetch bounds are out of range true empty false limit 10 stable 7 aborts false
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Fetch bounds and reported watermarks: follower read committed at log start
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower true committed true fetches offset 2
    Then fetch bounds are out of range false empty false limit 10 stable 7 aborts false
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Fetch bounds and reported watermarks: follower read committed before log end
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower true committed true fetches offset 9
    Then fetch bounds are out of range false empty false limit 10 stable 7 aborts false
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Fetch bounds and reported watermarks: follower read committed at log end
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower true committed true fetches offset 10
    Then fetch bounds are out of range false empty true limit 10 stable 7 aborts false
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Fetch bounds and reported watermarks: follower read committed beyond log end
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower true committed true fetches offset 11
    Then fetch bounds are out of range true empty false limit 10 stable 7 aborts false
    And fetch reports high watermark 8 and stable offset 7

  Scenario: Fetch bounds and reported watermarks: follower read committed maximum offset
    Given fetch watermarks start 2 high 8 stable 7 end 10 deliverable 8
    When a follower true committed true fetches offset 9223372036854775807
    Then fetch bounds are out of range true empty false limit 10 stable 7 aborts false
    And fetch reports high watermark 8 and stable offset 7
