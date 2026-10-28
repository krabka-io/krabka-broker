Feature: tests jvm acceptance cli topics

  Scenario: kafka topics describe smokes metadata
    Then jvm acceptance cli topics: kafka topics describe smokes metadata

  Scenario: kafka topics alter partitions
    Then jvm acceptance cli topics: kafka topics alter partitions

  Scenario: kafka topics delete removes the topic
    Then jvm acceptance cli topics: kafka topics delete removes the topic

  Scenario: kafka topics create accepts a compact and delete cleanup policy
    Then jvm acceptance cli topics: kafka topics create accepts a compact and delete cleanup policy
