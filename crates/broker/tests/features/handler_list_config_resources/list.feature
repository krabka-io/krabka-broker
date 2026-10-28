Feature: ListConfigResources applies membership filters and stable Kafka resource ordering
  Scenario Outline: Listings respect protocol version duplicate filters and listener-specific brokers
    Given metadata has topics t-b and t-a group g-1 subscription sub-1 plaintext brokers 10 and 2 and SSL-only broker 3
    And ListConfigResources version <version> requests types <types>
    When configuration resources are collected on the PLAINTEXT listener
    Then the complete configuration resource rows in order are <expected>
    Examples:
      | version | types               | expected                                                                                                                   |
      | 0       | all                 | client_metrics:sub-1                                                                                                       |
      | 1       | all                 | group:g-1,client_metrics:sub-1,broker_logger:2,broker_logger:10,broker:2,broker:10,topic:t-a,topic:t-b                         |
      | 1       | topic,topic         | topic:t-a,topic:t-b                                                                                                         |
      | 1       | topic,broker,group  | group:g-1,broker:2,broker:10,topic:t-a,topic:t-b                                                                               |
      | 1       | broker_logger       | broker_logger:2,broker_logger:10                                                                                            |
      | 1       | client_metrics      | client_metrics:sub-1                                                                                                       |
