Feature: WAL divergence responses distinguish Kafka sentinels from real epochs

  Scenario Outline: An absent divergence sentinel does not request truncation
    Given the WAL leader advertises diverging epoch <epoch> ending at <end>
    When the WAL follower validates the leader's diverging epoch
    Then the validated WAL divergence is absent

    Examples:
      | epoch | end |
      | -1 | -1 |
      | 3 | -1 |

  Scenario Outline: A real divergence retains its epoch and offset
    Given the WAL leader advertises diverging epoch <epoch> ending at <end>
    When the WAL follower validates the leader's diverging epoch
    Then the validated WAL divergence is epoch <epoch> at offset <end>

    Examples:
      | epoch | end |
      | 3 | 10 |
      | 0 | 0 |

  Scenario: A named end offset cannot carry a negative epoch
    Given the WAL leader advertises diverging epoch -1 ending at 10
    When the WAL follower validates the leader's diverging epoch
    Then WAL divergence validation fails with leader returned an invalid WAL diverging epoch
