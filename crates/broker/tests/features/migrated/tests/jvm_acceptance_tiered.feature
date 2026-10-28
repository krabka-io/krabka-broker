Feature: tests jvm acceptance tiered

  Scenario: tiered storage round trip through minio
    Then jvm acceptance tiered: tiered storage round trip through minio

  Scenario: tiered storage disable needs delete on disable
    Then jvm acceptance tiered: tiered storage disable needs delete on disable

  Scenario: tiered storage topic rlmm survives restart
    Then jvm acceptance tiered: tiered storage topic rlmm survives restart

  Scenario: restored cluster serves the jvm console consumer
    Then jvm acceptance tiered: restored cluster serves the jvm console consumer
