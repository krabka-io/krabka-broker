Feature: Intersect follower and leader epoch histories

  Scenario Outline: Preserve only the shared epoch prefix
    Given follower epoch/end, leader epoch/end and local end <input>
    When the follower calculates its divergence truncation target
    Then the divergence truncation offset is <offset>

    Examples:
      | input | offset |
      | [4,8,4,-1,9] | 9 |
      | [-1,-1,6,12,15] | 12 |
      | [-1,-1,6,20,15] | 15 |
      | [4,10,5,12,15] | 10 |
      | [4,10,5,12,8] | 8 |
      | [5,12,5,10,15] | 10 |
      | [5,10,5,12,15] | 10 |
      | [5,12,5,14,11] | 11 |
