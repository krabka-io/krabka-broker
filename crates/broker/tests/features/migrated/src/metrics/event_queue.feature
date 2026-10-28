Feature: Controller event queue recorders publish Kafka named series in seconds

  Background:
    Given a fresh controller event queue metrics registry

  Scenario: Nothing recorded initializes every series to zero
    When the Kafka named controller queue series are scraped
    Then the controller event queue metrics contain:
      | queue_count | queue_sum | processing_count | processing_sum | started | timed_out | idle_seconds |
      | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

  Scenario: Queue wait uses a seconds histogram
    When a controller operation waits 250 milliseconds in the queue
    When the Kafka named controller queue series are scraped
    Then the controller event queue metrics contain:
      | queue_count | queue_sum | processing_count | processing_sum | started | timed_out | idle_seconds |
      | 1 | 0.25 | 0 | 0 | 0 | 0 | 0 |

  Scenario: Processing times accumulate in seconds
    When a controller operation takes 500 milliseconds to process
    When a controller operation takes 250 milliseconds to process
    When the Kafka named controller queue series are scraped
    Then the controller event queue metrics contain:
      | queue_count | queue_sum | processing_count | processing_sum | started | timed_out | idle_seconds |
      | 0 | 0 | 2 | 0.75 | 0 | 0 | 0 |

  Scenario: Started and timed out operations have independent counters
    When a controller operation starts
    When a controller operation starts
    When a controller operation times out
    When the Kafka named controller queue series are scraped
    Then the controller event queue metrics contain:
      | queue_count | queue_sum | processing_count | processing_sum | started | timed_out | idle_seconds |
      | 0 | 0 | 0 | 0 | 2 | 1 | 0 |

  Scenario: Idle durations accumulate as an idle seconds counter
    When the controller queue is idle for 1500 milliseconds
    When the controller queue is idle for 500 milliseconds
    When the Kafka named controller queue series are scraped
    Then the controller event queue metrics contain:
      | queue_count | queue_sum | processing_count | processing_sum | started | timed_out | idle_seconds |
      | 0 | 0 | 0 | 0 | 0 | 0 | 2 |
