Feature: Kafka leader epoch assignment and start truncation
  Epoch histories use epoch:start-offset pairs.

  Scenario Outline: Epoch assignment updates only the history it supersedes
    Given a persisted leader epoch history <before>
    When leader epoch <epoch> is assigned to start offset <offset>
    Then the leader epoch entries in memory and on disk are <after>

    Examples:
      | case                                               | before      | epoch | offset | after       |
      | the first epoch of an empty history                | empty       | 2     | 6      | 2:6         |
      | a newer epoch at a later offset is appended        | 0:0,2:6     | 3     | 9      | 0:0,2:6,3:9 |
      | the latest epoch at a later offset is kept         | 2:6         | 2     | 7      | 2:6         |
      | the latest entry assigned again is kept            | 2:6         | 2     | 6      | 2:6         |
      | the latest epoch at an earlier offset moves back   | 1:0,2:6     | 2     | 5      | 1:0,2:5     |
      | a newer epoch at the same offset replaces it       | 1:0,2:6     | 3     | 6      | 1:0,3:6     |
      | an older epoch drops the entries it supersedes     | 1:0,2:6,3:8 | 2     | 7      | 1:0,2:7     |

  Scenario Outline: Start truncation retains the epoch covering the new log start
    Given a persisted leader epoch history <before>
    When the leader epoch history is truncated from start offset <offset>
    Then the leader epoch entries in memory and on disk are <after>

    Examples:
      | case                                  | before        | offset | after         |
      | an empty history                      | empty         | 7      | empty         |
      | an offset before the first entry      | 0:5,1:10,2:15 | 4      | 0:5,1:10,2:15 |
      | the start of the first entry          | 0:5,1:10,2:15 | 5      | 0:5,1:10,2:15 |
      | an offset inside the first epoch      | 0:5,1:10,2:15 | 8      | 0:8,1:10,2:15 |
      | the start of a later epoch            | 0:5,1:10,2:15 | 10     | 1:10,2:15     |
      | an offset past the latest epoch start | 0:5,1:10,2:15 | 17     | 2:17          |
