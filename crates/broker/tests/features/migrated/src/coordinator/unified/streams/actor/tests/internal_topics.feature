Feature: Streams internal topic partition sizing

  Scenario Outline: a join sizes the internal topics as kafka does
    Given a fresh streams actor with environment <environment>
    When a member joins a two-subtopology orders to rp to customers topology with copartition <copartitioned>
    Then the requested internal topics have names and partition counts <topics> with replication zero and no configs
    And the complete last streams heartbeat response is <expected>

    Examples:
      | environment | copartitioned | topics | expected |
      | {"topics":[["orders",1,6],["customers",2,3]],"max_size":10,"unstable":false} | true | [["rp",3],["store-changelog",3]] | {"code":0,"message":null,"member":"m1","epoch":2,"heartbeat_ms":5000,"task_ms":0,"lag":10000,"active":[],"standby":[],"warmup":[],"status":[[3,"Internal topics are missing: rp, store-changelog"]],"endpoint_epoch":0,"endpoints":null} |
      | {"topics":[["orders",1,4],["customers",2,8]],"max_size":10,"unstable":false} | false | [["rp",4],["store-changelog",8]] | {"code":0,"message":null,"member":"m1","epoch":2,"heartbeat_ms":5000,"task_ms":0,"lag":10000,"active":[],"standby":[],"warmup":[],"status":[[3,"Internal topics are missing: rp, store-changelog"]],"endpoint_epoch":0,"endpoints":null} |
