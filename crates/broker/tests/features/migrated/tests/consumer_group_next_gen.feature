Feature: tests consumer group next gen

  Scenario: single member full lifecycle
    Then consumer group next gen: single member full lifecycle

  Scenario: two members split partitions
    Then consumer group next gen: two members split partitions

  Scenario: classic group locked against next gen
    Then consumer group next gen: classic group locked against next gen

  Scenario: kill switch returns unsupported version
    Then consumer group next gen: kill switch returns unsupported version

  Scenario: describe after join
    Then consumer group next gen: describe after join

  Scenario: an old epoch is fenced
    Then consumer group next gen: an old epoch is fenced

  Scenario: first join with client member id echoes and assigns
    Then consumer group next gen: first join with client member id echoes and assigns

  Scenario: list groups includes next gen consumer group
    Then consumer group next gen: list groups includes next gen consumer group

  Scenario: an invalid subscribed topic regex fails the heartbeat
    Then consumer group next gen: an invalid subscribed topic regex fails the heartbeat

  Scenario: a topic created after the member joined reaches its next heartbeat
    Then consumer group next gen: a topic created after the member joined reaches its next heartbeat

  Scenario: a group config overrides the heartbeat interval of its group
    Then consumer group next gen: a group config overrides the heartbeat interval of its group
