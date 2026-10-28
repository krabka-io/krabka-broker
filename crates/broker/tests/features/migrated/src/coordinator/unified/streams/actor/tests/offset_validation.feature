Feature: Streams member identity fences offset access

  Scenario Outline: Validate a streams offset commit
    Given streams offset identity <input> with live member m1 at epoch 5
    When the streams group validates committing in partition 0
    Then the streams offset validation returns Kafka code <code>

    Examples:
      | input | code |
      | {"live":false,"member_id":"","instance_id":null,"epoch":-1,"offset_commit_version":9} | 0 |
      | {"live":true,"member_id":"","instance_id":null,"epoch":-1,"offset_commit_version":9} | 25 |
      | {"live":true,"member_id":"","instance_id":null,"epoch":5,"offset_commit_version":9} | 25 |
      | {"live":true,"member_id":"","instance_id":null,"epoch":-1,"offset_commit_version":null} | 0 |
      | {"live":true,"member_id":"","instance_id":"instance","epoch":-1,"offset_commit_version":null} | 25 |
      | {"live":false,"member_id":"","instance_id":"instance","epoch":-1,"offset_commit_version":null} | 0 |
      | {"live":true,"member_id":"m2","instance_id":null,"epoch":5,"offset_commit_version":9} | 25 |
      | {"live":true,"member_id":"m1","instance_id":null,"epoch":5,"offset_commit_version":8} | 35 |
      | {"live":true,"member_id":"m1","instance_id":null,"epoch":5,"offset_commit_version":null} | 0 |
      | {"live":true,"member_id":"m1","instance_id":null,"epoch":5,"offset_commit_version":9} | 0 |
      | {"live":true,"member_id":"m1","instance_id":null,"epoch":6,"offset_commit_version":9} | 113 |
      | {"live":true,"member_id":"m1","instance_id":null,"epoch":4,"offset_commit_version":9} | 113 |

  Scenario Outline: Validate a streams offset fetch
    Given streams offset identity <input> with live member m1 at epoch 5
    When the streams group validates fetching offsets
    Then the streams offset validation returns Kafka code <code>

    Examples:
      | input | code |
      | {"live":true,"member_id":null,"instance_id":null,"epoch":-1,"offset_commit_version":null} | 0 |
      | {"live":true,"member_id":"m1","instance_id":null,"epoch":5,"offset_commit_version":null} | 0 |
      | {"live":true,"member_id":"m1","instance_id":null,"epoch":4,"offset_commit_version":null} | 113 |
      | {"live":true,"member_id":"m1","instance_id":null,"epoch":6,"offset_commit_version":null} | 113 |
      | {"live":true,"member_id":"ghost","instance_id":null,"epoch":5,"offset_commit_version":null} | 25 |
      | {"live":true,"member_id":null,"instance_id":null,"epoch":5,"offset_commit_version":null} | 25 |
