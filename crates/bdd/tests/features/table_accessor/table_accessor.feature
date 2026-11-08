Feature: A step reads its data table into typed rows

  Scenario: Partitions created from a horizontal table keep their leader epochs
    When these partitions are created
      | topic  | partition | leader epoch |
      | orders | 0         | 5            |
      | orders | 1         | -            |
    Then partition orders-0 has leader epoch 5
    And partition orders-1 has no leader epoch

  Scenario: A topic created from a vertical table keeps every field
    When a topic is created from
      | field              | value  |
      | name               | orders |
      | partitions         | 3      |
      | replication factor | 2      |
    Then topic orders has 3 partitions and replication factor 2

  Scenario: A misspelled column is refused with the column it resembles
    When a step attempts to read these partitions
      | topic  | partiton |
      | orders | 0        |
    Then reading the table fails on unknown column partiton, suggesting partition
