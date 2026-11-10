Feature: Share state writes preserve Kafka epoch validation and stored-state rules
  Topic metadata contains partitions 0 and 1. Partition 0 starts at state epoch 2,
  leader epoch 3, offset 10 and delivery count 4. A previously initialized key for
  partition 2 remains stored at state epoch 2. Summaries list state epoch, leader
  epoch, start offset and delivery-complete count.

  Scenario Outline: Write stored share state under Kafka release rules
    Given a share coordinator stores group g at state epoch 2, leader epoch 3, offset 10 and delivery count 4
    And the WriteShareGroupState request and missing-log fault are JSON <request>
    When the coordinator writes the supplied share state and delivery range
    Then the share write outcome and complete stored summary are JSON <expected>

    Examples:
      | case | request | expected |
      | write request 1 | {"partition":0,"state_epoch":2,"leader_epoch":3,"start_offset":10,"delivery_complete_count":2,"drop_log":false} | {"outcome":{"kind":"Ok"},"summary":[2,3,10,4]} |
      | write request 2 | {"partition":0,"state_epoch":2,"leader_epoch":3,"start_offset":5,"delivery_complete_count":9,"drop_log":false} | {"outcome":{"kind":"Ok"},"summary":[2,3,10,4]} |
      | write request 3 | {"partition":0,"state_epoch":2,"leader_epoch":3,"start_offset":20,"delivery_complete_count":1,"drop_log":false} | {"outcome":{"kind":"Ok"},"summary":[2,3,20,1]} |
      | write request 4 | {"partition":0,"state_epoch":7,"leader_epoch":3,"start_offset":10,"delivery_complete_count":4,"drop_log":false} | {"outcome":{"kind":"Ok"},"summary":[2,3,10,4]} |
      | write request 5 | {"partition":0,"state_epoch":2,"leader_epoch":9,"start_offset":10,"delivery_complete_count":4,"drop_log":false} | {"outcome":{"kind":"Ok"},"summary":[2,3,10,4]} |
      | write request 6 | {"partition":0,"state_epoch":2,"leader_epoch":3,"start_offset":10,"delivery_complete_count":-1,"drop_log":false} | {"outcome":{"kind":"Ok"},"summary":[2,3,10,4]} |
      | write request 7 | {"partition":0,"state_epoch":1,"leader_epoch":3,"start_offset":10,"delivery_complete_count":4,"drop_log":false} | {"outcome":{"kind":"Refused","code":124,"message":"The coordinator rejected the request because the state epoch did not match."},"summary":[2,3,10,4]} |
      | write request 8 | {"partition":0,"state_epoch":2,"leader_epoch":2,"start_offset":10,"delivery_complete_count":4,"drop_log":false} | {"outcome":{"kind":"Refused","code":74,"message":"The leader epoch in the request is older than the epoch on the broker."},"summary":[2,3,10,4]} |
      | write request 9 | {"partition":1,"state_epoch":0,"leader_epoch":0,"start_offset":0,"delivery_complete_count":0,"drop_log":false} | {"outcome":{"kind":"Refused","code":42,"message":"Write operation on uninitialized share partition not allowed."},"summary":null} |
      | write request 10 | {"partition":-1,"state_epoch":2,"leader_epoch":3,"start_offset":10,"delivery_complete_count":4,"drop_log":false} | {"outcome":{"kind":"Refused","code":42,"message":"The partition id cannot be a negative number."},"summary":null} |
      | write request 11 | {"partition":0,"state_epoch":2,"leader_epoch":-1,"start_offset":10,"delivery_complete_count":4,"drop_log":false} | {"outcome":{"kind":"Refused","code":42,"message":"The leader epoch cannot be a negative number."},"summary":[2,3,10,4]} |
      | write request 12 | {"partition":0,"state_epoch":-1,"leader_epoch":3,"start_offset":10,"delivery_complete_count":4,"drop_log":false} | {"outcome":{"kind":"Refused","code":42,"message":"The state epoch cannot be a negative number."},"summary":[2,3,10,4]} |
      | write request 13 | {"partition":2,"state_epoch":2,"leader_epoch":3,"start_offset":10,"delivery_complete_count":4,"drop_log":false} | {"outcome":{"kind":"Refused","code":3,"message":"This server does not host this topic-partition."},"summary":[2,0,10,0]} |
      | write request 14 | {"partition":0,"state_epoch":2,"leader_epoch":3,"start_offset":30,"delivery_complete_count":0,"drop_log":true} | {"outcome":{"kind":"Operation","code":15,"message":"This server does not host this topic-partition."},"summary":[2,3,10,4]} |
