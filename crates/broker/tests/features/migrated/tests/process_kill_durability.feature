Feature: Process loss preserves acknowledged records under the default flush policy
  Scenario: The default durability premise uses page cache without per append fsync
    Given a broker log uses its default configuration
    When the per append flush policy is inspected
    Then per append fsync is disabled by default

  Scenario: A synthetic torn tail preserves the complete prefix and exposes a partial batch
    Given a segment contains one complete batch at offset 0 with last offset delta 3
    When the complete segment is walked batch by batch
    Then the walk reaches a boundary with one batch spanning bytes zero to 61 and offsets zero through 3
    When the first half of the last complete batch is appended as a torn tail
    Then the resulting segment preserves its complete batch and reports 30 partial trailing bytes

  @docker
  Scenario: Acknowledged records survive repeated SIGKILL and deterministic torn tail recovery
    Given the packaged single broker process hosts topic kill-durability with all acknowledgements and one in sync replica
    When cycle 0 acknowledges 20 settled records then SIGKILL interrupts a producer burst with tail mode intact
    Then the last cycle exits with SIGKILL status 137 acknowledges 20 settled records and recovers a continuous valid batch chain
    When all acknowledged records are read after cycle 0
    Then every key acknowledged across all completed cycles remains readable
    When cycle 1 acknowledges 20 settled records then SIGKILL interrupts a producer burst with tail mode torn
    Then the last cycle exits with SIGKILL status 137 acknowledges 20 settled records and recovers a continuous valid batch chain
    When all acknowledged records are read after cycle 1
    Then every key acknowledged across all completed cycles remains readable
    When cycle 2 acknowledges 20 settled records then SIGKILL interrupts a producer burst with tail mode intact
    Then the last cycle exits with SIGKILL status 137 acknowledges 20 settled records and recovers a continuous valid batch chain
    When all acknowledged records are read after cycle 2
    Then every key acknowledged across all completed cycles remains readable
    When cycle 3 acknowledges 20 settled records then SIGKILL interrupts a producer burst with tail mode torn
    Then the last cycle exits with SIGKILL status 137 acknowledges 20 settled records and recovers a continuous valid batch chain
    When all acknowledged records are read after cycle 3
    Then every key acknowledged across all completed cycles remains readable
    When cycle 4 acknowledges 20 settled records then SIGKILL interrupts a producer burst with tail mode intact
    Then the last cycle exits with SIGKILL status 137 acknowledges 20 settled records and recovers a continuous valid batch chain
    When all acknowledged records are read after cycle 4
    Then every key acknowledged across all completed cycles remains readable
    When cycle 5 acknowledges 20 settled records then SIGKILL interrupts a producer burst with tail mode torn
    Then the last cycle exits with SIGKILL status 137 acknowledges 20 settled records and recovers a continuous valid batch chain
    When all acknowledged records are read after cycle 5
    Then every key acknowledged across all completed cycles remains readable
    And 6 kill restart cycles include 3 observed partial tail faults
