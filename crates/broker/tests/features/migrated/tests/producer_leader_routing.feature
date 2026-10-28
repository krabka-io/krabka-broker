Feature: Native producer routing to remote leaders
  Scenario: A bootstrap producer stores writes on every partition leader
    Given topic producer-routing-rf1 has six single-replica partitions across three brokers including non-bootstrap leaders
    And one idempotent producer bootstraps only at broker 1 with all acknowledgements
    When that producer enqueues and flushes these partition-pinned records
      | partition | value |
      | 0         | p0    |
      | 1         | p1    |
      | 2         | p2    |
      | 3         | p3    |
      | 4         | p4    |
      | 5         | p5    |
    Then all 6 records are acknowledged for their requested partitions
    When an earliest native consumer reads all six records through partition leaders
    Then every partition including non-bootstrap partitions contains these exact values
      | partition | value |
      | 0         | p0    |
      | 1         | p1    |
      | 2         | p2    |
      | 3         | p3    |
      | 4         | p4    |
      | 5         | p5    |
