Feature: tests jvm bootstrap controller

  Scenario: kafka configs bootstrap controller round trips a dynamic broker config
    Then jvm bootstrap controller: kafka configs bootstrap controller round trips a dynamic broker config

  Scenario: kafka acls bootstrap controller add list remove
    Then jvm bootstrap controller: kafka acls bootstrap controller add list remove

  Scenario: kafka reassign partitions bootstrap controller lists
    Then jvm bootstrap controller: kafka reassign partitions bootstrap controller lists

  Scenario: kafka topics has no bootstrap controller option
    Then jvm bootstrap controller: kafka topics has no bootstrap controller option

  Scenario: kafka features bootstrap controller describes and downgrades
    Then jvm bootstrap controller: kafka features bootstrap controller describes and downgrades
