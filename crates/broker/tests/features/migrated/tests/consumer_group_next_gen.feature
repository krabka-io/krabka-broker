Feature: KIP-848 consumer membership through native broker RPCs

  Scenario: single member full lifecycle
    Given a consumer broker with protocol policy classic-and-consumer and topics [["t1",4]]
    When member A sends consumer heartbeat {"group":"g1","id":null,"epoch":0,"topics":["t1"],"regex":null,"ownership":"empty"}
    Then member A receives Kafka code 0
    And member A receives epoch 2
    And member A receives 4 assigned partitions
    When member A sends consumer heartbeat {"group":"g1","id":null,"epoch":2,"topics":["t1"],"regex":null,"ownership":"omitted"}
    Then member A receives Kafka code 0
    And member A receives epoch 2
    When member A sends consumer heartbeat {"group":"g1","id":null,"epoch":-1,"topics":null,"regex":null,"ownership":"omitted"}
    Then member A receives Kafka code 0
    And the consumer behavior broker is shut down

  Scenario: two members split partitions
    Given a consumer broker with protocol policy classic-and-consumer and topics [["t2",4]]
    When member A sends consumer heartbeat {"group":"g2","id":null,"epoch":0,"topics":["t2"],"regex":null,"ownership":"empty"}
    Then member A receives Kafka code 0
    When member B sends consumer heartbeat {"group":"g2","id":null,"epoch":0,"topics":["t2"],"regex":null,"ownership":"empty"}
    Then member B receives Kafka code 0
    When member A sends consumer heartbeat {"group":"g2","id":null,"epoch":null,"topics":["t2"],"regex":null,"ownership":"previous-assignment"}
    Then member A receives Kafka code 0
    When member A sends consumer heartbeat {"group":"g2","id":null,"epoch":null,"topics":["t2"],"regex":null,"ownership":"previous-assignment"}
    Then member A receives Kafka code 0
    When member B sends consumer heartbeat {"group":"g2","id":null,"epoch":null,"topics":["t2"],"regex":null,"ownership":"previous-assignment"}
    Then member B receives Kafka code 0
    And member A response 1 and member B response 1 assign 4 partitions in total
    And the consumer behavior broker is shut down

  Scenario: classic group locked against next gen
    Given a consumer broker with protocol policy classic-and-consumer and topics [["t3",2]]
    And a live classic group g3 uses protocol type connect with protocol default
    When member A sends consumer heartbeat {"group":"g3","id":null,"epoch":0,"topics":["t3"],"regex":null,"ownership":"empty"}
    Then member A receives Kafka code 69
    And member A receives exact message Cannot upgrade classic group g3 to consumer group because the group does not use the consumer embedded protocol.
    And the consumer behavior broker is shut down

  Scenario: kill switch returns unsupported version
    Given a consumer broker with protocol policy classic-only and topics []
    When member A sends consumer heartbeat {"group":"g4","id":null,"epoch":0,"topics":["t"],"regex":null,"ownership":"empty"}
    Then member A receives Kafka code 35
    And the consumer behavior broker is shut down

  Scenario: describe after join
    Given a consumer broker with protocol policy classic-and-consumer and topics [["t5",2]]
    When member A sends consumer heartbeat {"group":"g5","id":null,"epoch":0,"topics":["t5"],"regex":null,"ownership":"empty"}
    When the client describes consumer group g5
    Then the description contains 1 group with Kafka code 0 and state Stable
    And the consumer behavior broker is shut down

  Scenario: an old epoch is fenced
    Given a consumer broker with protocol policy classic-and-consumer and topics [["t6",2]]
    When member A sends consumer heartbeat {"group":"g6","id":null,"epoch":0,"topics":["t6"],"regex":null,"ownership":"empty"}
    Then member A receives Kafka code 0
    When member B sends consumer heartbeat {"group":"g6","id":null,"epoch":0,"topics":["t6"],"regex":null,"ownership":"empty"}
    Then member B receives Kafka code 0
    When member A sends consumer heartbeat {"group":"g6","id":null,"epoch":2,"topics":["t6"],"regex":null,"ownership":"omitted"}
    Then member A receives Kafka code 0
    And member A receives epoch 2
    When member A sends consumer heartbeat {"group":"g6","id":null,"epoch":2,"topics":null,"regex":null,"ownership":"previous-assignment"}
    Then member A receives Kafka code 0
    And member A receives epoch 3
    When member A sends consumer heartbeat {"group":"g6","id":null,"epoch":2,"topics":null,"regex":null,"ownership":"omitted"}
    Then member A receives Kafka code 110
    And the consumer behavior broker is shut down

  Scenario: first join with client member id echoes and assigns
    Given a consumer broker with protocol policy classic-and-consumer and topics [["tc",2]]
    When member A sends consumer heartbeat {"group":"gc","id":"client-generated-id","epoch":0,"topics":["tc"],"regex":null,"ownership":"empty"}
    Then member A receives Kafka code 0
    And member A receives id client-generated-id
    And member A receives 2 assigned partitions
    And the consumer behavior broker is shut down

  Scenario: list groups includes next gen consumer group
    Given a consumer broker with protocol policy classic-and-consumer and topics [["tlist",2]]
    When member A sends consumer heartbeat {"group":"glist","id":null,"epoch":0,"topics":["tlist"],"regex":null,"ownership":"empty"}
    Then member A receives Kafka code 0
    When the client lists groups with type filter consumer
    Then group glist appears 1 times with type consumer
    When the client lists groups with type filter share
    Then group glist appears 0 times with type consumer
    When the client lists groups with type filter all
    Then group glist appears 1 times with type consumer
    And the consumer behavior broker is shut down

  Scenario: an invalid subscribed topic regex fails the heartbeat
    Given a consumer broker with protocol policy classic-and-consumer and topics [["t-badregex",1]]
    When member 0 sends consumer heartbeat {"group":"g-badregex","id":null,"epoch":0,"topics":null,"regex":"(","ownership":"empty"}
    Then member 0 receives Kafka code 128
    And member 0 receives diagnostic fragment is not a valid regular expression without admission
    When member 1 sends consumer heartbeat {"group":"g-badregex","id":null,"epoch":0,"topics":null,"regex":"[a-","ownership":"empty"}
    Then member 1 receives Kafka code 128
    And member 1 receives diagnostic fragment is not a valid regular expression without admission
    When member 2 sends consumer heartbeat {"group":"g-badregex","id":null,"epoch":0,"topics":null,"regex":"a{2,1}","ownership":"empty"}
    Then member 2 receives Kafka code 128
    And member 2 receives diagnostic fragment is not a valid regular expression without admission
    When member good sends consumer heartbeat {"group":"g-badregex","id":null,"epoch":0,"topics":null,"regex":"t-bad.*","ownership":"empty"}
    Then member good receives Kafka code 0
    And member good receives epoch 1
    And the consumer behavior broker is shut down

  Scenario: a topic created after the member joined reaches its next heartbeat
    Given a consumer broker with protocol policy classic-and-consumer and topics []
    When member A sends consumer heartbeat {"group":"g-late","id":null,"epoch":0,"topics":["late"],"regex":null,"ownership":"empty"}
    Then member A receives Kafka code 0
    And member A receives epoch 2
    And member A receives an explicit empty assignment
    When topic late is created with 3 partitions after member A joined group g-late at epoch 2
    Then member A receives the complete new-topic response at epoch 3 interval 5000 and partitions [0,1,2]
    And the consumer behavior broker is shut down

  Scenario: a group config overrides the heartbeat interval of its group
    Given a consumer broker with protocol policy classic-and-consumer and topics []
    And group g-tuned has persisted consumer configuration {"consumer.heartbeat.interval.ms":"7000","consumer.session.timeout.ms":"50000"}
    When member tuned sends consumer heartbeat {"group":"g-tuned","id":null,"epoch":0,"topics":[],"regex":null,"ownership":"empty"}
    Then member tuned receives Kafka code 0
    And member tuned receives heartbeat interval 7000 milliseconds
    When member plain sends consumer heartbeat {"group":"g-plain","id":null,"epoch":0,"topics":[],"regex":null,"ownership":"empty"}
    Then member plain receives Kafka code 0
    And member plain receives heartbeat interval 5000 milliseconds
    And the consumer behavior broker is shut down
