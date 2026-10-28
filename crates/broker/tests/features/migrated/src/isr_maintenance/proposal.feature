Feature: ISR maintenance follows Kafka follower lag and eligibility rules
  A scan shrinks followers that lost progress and admits caught-up replicas only
  when their fetch proves the current registered broker epoch.

  Background:
    Given brokers 1, 2 and 3 are unfenced at broker epoch 5 with ISR minimum 1 and lag limit 1000 milliseconds
    And leader 1 has leader epoch 7, partition epoch 3, log end 100, high watermark 90 and epoch start 80

  Scenario Outline: Scan when <case>
    Given the installed ISR, assigned replicas and follower observations are JSON <members>
    When the leader scans for Kafka ISR expansion or shrink
    Then the complete ISR proposal is JSON <proposal>

    Examples:
      | case | members | proposal |
      | every follower caught up recently | {"isr":[1,2,3],"replicas":[1,2,3],"followers":[{"node":2,"leo":90,"last_fetch_ms_ago":10,"last_caught_up_ms_ago":500,"broker_epoch":5},{"node":3,"leo":80,"last_fetch_ms_ago":10,"last_caught_up_ms_ago":1000,"broker_epoch":5}]} | null |
      | a follower fetching but not caught up for too long leaves | {"isr":[1,2,3],"replicas":[1,2,3],"followers":[{"node":2,"leo":90,"last_fetch_ms_ago":10,"last_caught_up_ms_ago":10,"broker_epoch":5},{"node":3,"leo":80,"last_fetch_ms_ago":10,"last_caught_up_ms_ago":1001,"broker_epoch":5}]} | {"prev_isr":[1,2,3],"new_isr":[1,2],"leader_epoch":7,"partition_epoch":3} |
      | an idle follower at the leader's log end stays | {"isr":[1,2],"replicas":[1,2],"followers":[{"node":2,"leo":100,"last_fetch_ms_ago":30000,"last_caught_up_ms_ago":30000,"broker_epoch":5}]} | null |
      | a follower that never fetched from this leader leaves after the bound | {"isr":[1,2],"replicas":[1,2],"followers":[{"node":2,"leo":100,"last_fetch_ms_ago":null,"last_caught_up_ms_ago":1001,"broker_epoch":5}]} | {"prev_isr":[1,2],"new_isr":[1],"leader_epoch":7,"partition_epoch":3} |
      | an out-of-sync follower at the high watermark joins | {"isr":[1,2],"replicas":[1,2,3],"followers":[{"node":2,"leo":100,"last_fetch_ms_ago":10,"last_caught_up_ms_ago":10,"broker_epoch":5},{"node":3,"leo":90,"last_fetch_ms_ago":30000,"last_caught_up_ms_ago":30000,"broker_epoch":5}]} | {"prev_isr":[1,2],"new_isr":[1,2,3],"leader_epoch":7,"partition_epoch":3} |
      | an out-of-sync follower one short of the high watermark stays out | {"isr":[1,2],"replicas":[1,2,3],"followers":[{"node":2,"leo":100,"last_fetch_ms_ago":10,"last_caught_up_ms_ago":10,"broker_epoch":5},{"node":3,"leo":89,"last_fetch_ms_ago":10,"last_caught_up_ms_ago":10,"broker_epoch":5}]} | null |
      | a follower past the high watermark that has not fetched from this leader stays out | {"isr":[1,2],"replicas":[1,2,3],"followers":[{"node":2,"leo":100,"last_fetch_ms_ago":10,"last_caught_up_ms_ago":10,"broker_epoch":5},{"node":3,"leo":100,"last_fetch_ms_ago":null,"last_caught_up_ms_ago":10,"broker_epoch":5}]} | null |
      | an out-of-sync replica with no progress stays out | {"isr":[1,2],"replicas":[1,2,3],"followers":[{"node":2,"leo":100,"last_fetch_ms_ago":10,"last_caught_up_ms_ago":10,"broker_epoch":5}]} | null |
      | a follower whose fetch carried a stale broker epoch stays out | {"isr":[1,2],"replicas":[1,2,3],"followers":[{"node":2,"leo":100,"last_fetch_ms_ago":10,"last_caught_up_ms_ago":10,"broker_epoch":5},{"node":3,"leo":100,"last_fetch_ms_ago":10,"last_caught_up_ms_ago":10,"broker_epoch":4}]} | null |
      | the leader's own lag and a duplicate assignment change nothing | {"isr":[1,1,2,2],"replicas":[1,1,2],"followers":[{"node":1,"leo":0,"last_fetch_ms_ago":30000,"last_caught_up_ms_ago":30000,"broker_epoch":5},{"node":2,"leo":100,"last_fetch_ms_ago":10,"last_caught_up_ms_ago":10,"broker_epoch":5}]} | null |
