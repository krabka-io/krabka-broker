Feature: Kafka leader epoch assignment and start truncation
  Epoch histories use epoch:start-offset pairs.

  Scenario Outline: Epoch assignment updates only the history it supersedes
    Given a persisted leader epoch history <before>
    When leader epoch <epoch> is assigned to start offset <offset>
    Then the leader epoch entries in memory and on disk are <after>

    Examples:
      | before      | epoch | offset | after        |
      | empty       | 2     | 6      | 2:6          |
      | 0:0,2:6     | 3     | 9      | 0:0,2:6,3:9  |
      | 2:6         | 2     | 7      | 2:6          |
      | 2:6         | 2     | 6      | 2:6          |
      | 1:0,2:6     | 2     | 5      | 1:0,2:5      |
      | 1:0,2:6     | 3     | 6      | 1:0,3:6      |
      | 1:0,2:6,3:8 | 2     | 7      | 1:0,2:7      |

  Scenario Outline: Start truncation retains the epoch covering the new log start
    Given a persisted leader epoch history <before>
    When the leader epoch history is truncated from start offset <offset>
    Then the leader epoch entries in memory and on disk are <after>

    Examples:
      | before         | offset | after          |
      | empty          | 7      | empty          |
      | 0:5,1:10,2:15   | 4      | 0:5,1:10,2:15  |
      | 0:5,1:10,2:15   | 5      | 0:5,1:10,2:15  |
      | 0:5,1:10,2:15   | 8      | 0:8,1:10,2:15  |
      | 0:5,1:10,2:15   | 10     | 1:10,2:15      |
      | 0:5,1:10,2:15   | 17     | 2:17           |
