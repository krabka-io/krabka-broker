Feature: Transactional commits to streams groups

  Scenario Outline: Validate transactional streams membership including a respawned actor
    Given a streams member m1 at epoch 5 and transactional commit input <input>
    When the producer transactionally commits topic a partition 0 using API version 5
    Then the complete commit response is topic a partition 0 with Kafka code <code>

    Examples:
      | input | code |
      | {"stopped":false,"member_id":"","group_instance_id":null,"member_epoch":-1} | 0 |
      | {"stopped":false,"member_id":"","group_instance_id":"instance","member_epoch":-1} | 25 |
      | {"stopped":false,"member_id":"m1","group_instance_id":null,"member_epoch":5} | 0 |
      | {"stopped":true,"member_id":"","group_instance_id":null,"member_epoch":-1} | 0 |
      | {"stopped":true,"member_id":"m1","group_instance_id":null,"member_epoch":5} | 25 |
