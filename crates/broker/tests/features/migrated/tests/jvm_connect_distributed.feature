Feature: Distributed Kafka Connect file transfer
  Scenario: connect distributed worker round trips a file through krabka
    Given a host broker exposes topic krabka-connect-file and this source file
      """
      connect-line-1
      connect-line-2
      connect-line-3
      """
    When the Kafka 4.3.1 distributed worker starts with compacted config, offset and status stores
    Then the worker serves its REST API after replaying internal stores and joining its group
    When connector krabka-file-source is configured through REST
      """
      {"connector.class":"org.apache.kafka.connect.file.FileStreamSourceConnector","tasks.max":"1","file":"/tmp/connect-data/source.txt","topic":"krabka-connect-file"}
      """
    And connector krabka-file-sink is configured through REST
      """
      {"connector.class":"org.apache.kafka.connect.file.FileStreamSinkConnector","tasks.max":"1","file":"/tmp/connect-data/sink.txt","topics":"krabka-connect-file"}
      """
    Then each connector configuration is accepted and each connector and its one task reach RUNNING
    And the sink file eventually contains exactly these source lines
      | line           |
      | connect-line-1 |
      | connect-line-2 |
      | connect-line-3 |
    When the operator describes these Connect store topics
      | topic                  |
      | krabka-connect-configs  |
      | krabka-connect-offsets  |
      | krabka-connect-status   |
    Then all three internal topics report cleanup.policy=compact
