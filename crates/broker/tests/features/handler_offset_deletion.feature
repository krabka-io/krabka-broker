# Quoted <empty> denotes a zero-length Kafka string; an empty created-topic list contains no names.
Feature: Offset deletion authorizes groups and preserves subscribed offsets

  Scenario: Group Delete denial returns only a top-level error
    Given an offset deletion broker with group Delete allowed false
    When alice deletes offsets for group "classic-g" and these topic partitions
      | topic | partitions |
      | t | 0 |
    Then the complete offset deletion response has top-level code 30 and these ordered topic partitions
      | topic | partition | code |

  Scenario: Empty group IDs are invalid
    Given an offset deletion broker with group Delete allowed true
    And topics t and u have one partition, classic-g subscribes to t and ghost and commits u-0 at offset 5, connect-g speaks connect, garbled-g has consumer metadata byte 00 and kip848-g resolves regex t.* to t
    When alice deletes offsets for group "<empty>" and these topic partitions
      | topic | partitions |
      | t | 0 |
    Then the complete offset deletion response has top-level code 24 and these ordered topic partitions
      | topic | partition | code |

  Scenario: Missing groups return only a top-level error
    Given an offset deletion broker with group Delete allowed true
    And topics t and u have one partition, classic-g subscribes to t and ghost and commits u-0 at offset 5, connect-g speaks connect, garbled-g has consumer metadata byte 00 and kip848-g resolves regex t.* to t
    When alice deletes offsets for group "ghost-g" and these topic partitions
      | topic | partitions |
      | t | 0 |
    Then the complete offset deletion response has top-level code 69 and these ordered topic partitions
      | topic | partition | code |

  Scenario: A non-consumer nonempty classic group cannot delete offsets
    Given an offset deletion broker with group Delete allowed true
    And topics t and u have one partition, classic-g subscribes to t and ghost and commits u-0 at offset 5, connect-g speaks connect, garbled-g has consumer metadata byte 00 and kip848-g resolves regex t.* to t
    When alice deletes offsets for group "connect-g" and these topic partitions
      | topic | partitions |
      | t | 0 |
    Then the complete offset deletion response has top-level code 68 and these ordered topic partitions
      | topic | partition | code |

  Scenario: Undecodable consumer metadata protects every topic
    Given an offset deletion broker with group Delete allowed true
    And topics t and u have one partition, classic-g subscribes to t and ghost and commits u-0 at offset 5, connect-g speaks connect, garbled-g has consumer metadata byte 00 and kip848-g resolves regex t.* to t
    When alice deletes offsets for group "garbled-g" and these topic partitions
      | topic | partitions |
      | u | 0 |
    Then the complete offset deletion response has top-level code 0 and these ordered topic partitions
      | topic | partition | code |
      | u | 0 | 86 |

  Scenario: Resolved consumer regex subscriptions protect matching topics
    Given an offset deletion broker with group Delete allowed true
    And topics t and u have one partition, classic-g subscribes to t and ghost and commits u-0 at offset 5, connect-g speaks connect, garbled-g has consumer metadata byte 00 and kip848-g resolves regex t.* to t
    When alice deletes offsets for group "kip848-g" and these topic partitions
      | topic | partitions |
      | t | 0 |
    Then the complete offset deletion response has top-level code 0 and these ordered topic partitions
      | topic | partition | code |
      | t | 0 | 86 |

  Scenario: Existence precedes subscription and successful deletions write clock-dated tombstones
    Given an offset deletion broker with group Delete allowed true
    And topics t and u have one partition, classic-g subscribes to t and ghost and commits u-0 at offset 5, connect-g speaks connect, garbled-g has consumer metadata byte 00 and kip848-g resolves regex t.* to t
    When alice deletes offsets for group "classic-g" and these topic partitions
      | topic | partitions |
      | t | 0 |
      | ghost | 0 |
      | u | 0,5 |
    Then the complete offset deletion response has top-level code 0 and these ordered topic partitions
      | topic | partition | code |
      | ghost | 0 | 3 |
      | u | 5 | 3 |
      | u | 0 | 0 |
      | t | 0 | 86 |
