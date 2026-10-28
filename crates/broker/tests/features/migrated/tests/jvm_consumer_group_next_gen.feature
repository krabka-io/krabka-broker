Feature: tests jvm consumer group next gen

  Scenario: jvm kip848 single consumer round trip
    Then jvm consumer group next gen: jvm kip848 single consumer round trip

  Scenario: jvm kip848 describe group
    Then jvm consumer group next gen: jvm kip848 describe group

  Scenario: jvm kip848 delete group
    Then jvm consumer group next gen: jvm kip848 delete group

  Scenario: jvm kip848 coexists with classic
    Then jvm consumer group next gen: jvm kip848 coexists with classic

  Scenario: jvm kip848 classic and consumer in one group migrate
    Then jvm consumer group next gen: jvm kip848 classic and consumer in one group migrate
