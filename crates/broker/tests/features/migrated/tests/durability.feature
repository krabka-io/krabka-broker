Feature: Native broker durability, acknowledgment gates and committed reads

  Scenario: idempotent retry reappends after truncation instead of stalling
    Given a durability topic trunc on 1 brokers with 1 partitions and replication factor 1
    When the client produces durability batch {"values":["a","b","c"],"acks":-1,"timeout":5000,"producer_id":42,"sequence":0}
    Then the durability produce succeeds
    When the leader truncates its log to the returned batch base offset
    When the client produces durability batch {"values":["a","b","c"],"acks":-1,"timeout":3000,"producer_id":42,"sequence":0}
    Then the durability produce succeeds
    And the durability brokers are shut down

  Scenario: disk backed restart recovers idempotent producer state
    Given a durability topic restart-dedup on 1 brokers with 1 partitions and replication factor 1
    When the client produces durability batch {"values":["a"],"acks":-1,"timeout":5000,"producer_id":42,"sequence":0}
    Then the durability produce succeeds at base offset 0
    When the disk-backed broker restarts in Rejoin mode and leads its partition
    When the client produces durability batch {"values":["a"],"acks":-1,"timeout":5000,"producer_id":42,"sequence":0}
    Then the durability produce succeeds at base offset 0
    When the client produces durability batch {"values":["b"],"acks":-1,"timeout":5000,"producer_id":42,"sequence":1}
    Then the durability produce succeeds at base offset 1
    And the durability brokers are shut down

  Scenario: acks one returns quickly on rf1 broker
    Given a durability topic ack1 on 1 brokers with 1 partitions and replication factor 1
    When the client produces durability batch {"values":["a","b","c"],"acks":1,"timeout":5000,"producer_id":null,"sequence":0}
    Then the durability produce succeeds at base offset 0
    And the produce duration is at least 0 and below 1000 milliseconds
    And the durability brokers are shut down

  Scenario: acks all returns quickly on rf1 broker
    Given a durability topic ackall on 1 brokers with 1 partitions and replication factor 1
    When the client produces durability batch {"values":["a","b","c"],"acks":-1,"timeout":5000,"producer_id":null,"sequence":0}
    Then the durability produce succeeds at base offset 0
    And the produce duration is at least 0 and below 1000 milliseconds
    And the durability brokers are shut down

  Scenario: consumer clamps at hw when followers lag
    Given a durability topic clamp on 1 brokers with 1 partitions and replication factor 1
    When the client produces durability batch {"values":["x","y","z"],"acks":1,"timeout":5000,"producer_id":null,"sequence":0}
    Then the durability produce succeeds at base offset 0
    When the RF1 high watermark reaches 3 and a consumer fetches partition zero
    Then the consumer fetch returns Kafka code 0 and high watermark 3
    And the durability brokers are shut down

  Scenario: read committed under rf1 unchanged
    Given a durability topic rctxn on 1 brokers with 1 partitions and replication factor 1
    When transaction rc-tid commits payloads ["p","q","r"] and read-committed group rc-g polls for ten seconds every 200 milliseconds
    Then the read-committed consumer sees exactly ["p","q","r"]
    And the durability brokers are shut down

  Scenario: acks all completes via isr shrink when follower dead
    Given a durability topic shrink on 3 brokers with 1 partitions and replication factor 3
    When the last in-sync follower is shut down
    When the client produces durability batch {"values":["x","y","z"],"acks":-1,"timeout":10000,"producer_id":null,"sequence":0}
    Then the durability produce succeeds at base offset 0
    And the produce duration is at least 1500 and below 5000 milliseconds
    And the durability brokers are shut down

  Scenario: acks all stalled follower wait lands in remote time not local time
    Given a durability topic phase-stall on 3 brokers with 1 partitions and replication factor 3
    When the last in-sync follower is shut down
    When the client produces durability batch {"values":["x","y","z"],"acks":-1,"timeout":10000,"producer_id":null,"sequence":0}
    Then the durability produce succeeds at base offset 0
    When the leader's Produce local and remote duration sums are scraped
    Then remote Produce time is at least 0.5 seconds and exceeds local time by factor 4
    And the durability brokers are shut down

  Scenario: acks all waits for every partition at once not one after another
    Given a durability topic overlap on 2 brokers with 8 partitions and replication factor 2
    When the last in-sync follower is shut down
    And one acks-all request writes 8 partitions with timeout 1000 milliseconds
    Then all 8 partition rows return and at least 2 carry Kafka error 7
    And the produce duration is at least 1000 and below 2000 milliseconds
    And the durability brokers are shut down
