Feature: Recover out-of-range follower offsets
  # null is a failed leader lookup; -1 asks LATEST and -2 asks EARLIEST.

  Scenario Outline: Recover a replica after its fetch offset is rejected
    Given a replica with offsets 0 through 5 and leader lookups <lookups>
    When the follower recovers from OFFSET_OUT_OF_RANGE
    Then the recovery action, lookup order and log bounds are <expected>

    Examples:
      | lookups | expected |
      | [4,0] | {"action":"continue","asked":[-1],"log_start":0,"log_end":4} |
      | [20,12] | {"action":"continue","asked":[-1,-2],"log_start":12,"log_end":12} |
      | [20,3] | {"action":"continue","asked":[-1,-2],"log_start":0,"log_end":6} |
      | [20,6] | {"action":"continue","asked":[-1,-2],"log_start":0,"log_end":6} |
      | [6,0] | {"action":"continue","asked":[-1,-2],"log_start":0,"log_end":6} |
      | [null,0] | {"action":"unexpected-error-backoff","asked":[-1],"log_start":0,"log_end":6} |
      | [20,null] | {"action":"unexpected-error-backoff","asked":[-1,-2],"log_start":0,"log_end":6} |
