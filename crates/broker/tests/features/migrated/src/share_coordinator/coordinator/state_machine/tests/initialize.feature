Feature: Existing share state initialization follows Kafka release fencing and snapshot rules
  A summary lists state epoch, leader epoch, start offset and delivery-complete count.

  Scenario Outline: Initialize an existing key under Kafka release rules
    Given a share coordinator with trunk rules <trunk> initialized group g at state epoch 5 and start offset 100
    And the repeated InitializeShareGroupState request is JSON <request>
    When the coordinator initializes that stored share state key again
    Then the initialization outcome, persisted snapshot count and stored summary are JSON <expected>

    Examples:
      | case | trunk | request | expected |
      | a repeat of the stored epoch and start offset | false | {"state_epoch":5,"start_offset":100} | {"outcome":{"kind":"Ok"},"summary":[5,0,100,0],"snapshots":2} |
      | a repeat of the stored epoch and start offset | true | {"state_epoch":5,"start_offset":100} | {"outcome":{"kind":"Ok"},"summary":[5,0,100,0],"snapshots":1} |
      | a state epoch of -1 | false | {"state_epoch":-1,"start_offset":100} | {"outcome":{"kind":"Ok"},"summary":[-1,0,100,0],"snapshots":2} |
      | a state epoch of -1 | true | {"state_epoch":-1,"start_offset":100} | {"outcome":{"kind":"Refused","code":42,"message":"The state epoch cannot be a negative number."},"summary":[5,0,100,0],"snapshots":1} |
      | a lower state epoch | false | {"state_epoch":4,"start_offset":100} | {"outcome":{"kind":"Refused","code":124,"message":"The coordinator rejected the request because the state epoch did not match."},"summary":[5,0,100,0],"snapshots":1} |
      | a lower state epoch | true | {"state_epoch":4,"start_offset":100} | {"outcome":{"kind":"Refused","code":124,"message":"The coordinator rejected the request because the state epoch did not match."},"summary":[5,0,100,0],"snapshots":1} |
      | a higher state epoch and a new start offset | false | {"state_epoch":6,"start_offset":50} | {"outcome":{"kind":"Ok"},"summary":[6,0,50,0],"snapshots":2} |
      | a higher state epoch and a new start offset | true | {"state_epoch":6,"start_offset":50} | {"outcome":{"kind":"Ok"},"summary":[6,0,50,0],"snapshots":2} |
