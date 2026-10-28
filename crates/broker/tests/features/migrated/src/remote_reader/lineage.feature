Feature: Remote reads exclude segments from a diverging leader history
  A remote segment must follow every overlapping leader epoch and end before
  the leader's log end before a by-timestamp read can use it.

  Background:
    Given the leader epoch starts are JSON [[0,0],[1,10],[2,20],[3,30],[4,40],[5,50],[7,70]] and the leader log end is 90

  Scenario Outline: Admit a remote segment that <case>
    Given a remote segment has epoch starts JSON <epochs> and inclusive end offset <end>
    When the remote reader checks whether the segment belongs to the leader history
    Then the remote segment belongs to the current leader lineage

    Examples:
      | case | epochs | end |
      | several epochs within the lineage | [[1,15],[2,20],[3,30]] | 35 |
      | one epoch within the lineage | [[1,15]] | 19 |
      | starts at the lineage's first epoch start | [[0,0]] | 5 |
      | starts at the lineage's last epoch start | [[7,70]] | 75 |
      | ends at the last offset of its last epoch | [[1,15],[2,20]] | 29 |

  Scenario Outline: Refuse a remote segment that <case>
    Given a remote segment has epoch starts JSON <epochs> and inclusive end offset <end>
    When the remote reader checks whether the segment belongs to the leader history
    Then the remote segment is refused as outside the current leader lineage

    Examples:
      | case | epochs | end |
      | an epoch the lineage does not hold | [[5,55],[6,60],[7,70]] | 85 |
      | skips an epoch the lineage holds | [[1,15],[2,20],[4,40]] | 45 |
      | ends past the log end offset | [[0,0],[1,10],[2,20],[3,30],[4,40],[5,50],[7,70]] | 95 |
      | ends at the log end offset | [[0,0],[1,10],[2,20],[3,30],[4,40],[5,50],[7,70]] | 90 |
      | starts before its first epoch's start | [[1,5],[2,20]] | 25 |
      | ends past its last epoch's end | [[1,15],[2,20]] | 35 |
