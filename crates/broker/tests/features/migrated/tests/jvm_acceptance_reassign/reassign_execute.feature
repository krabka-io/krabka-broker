Feature: tests jvm acceptance reassign reassign execute

  Scenario: jvm kafka reassign partitions end to end
    Then jvm acceptance reassign reassign execute: jvm kafka reassign partitions end to end

  Scenario: reassign partitions generate produces a usable plan on both
    Then jvm acceptance reassign reassign execute: reassign partitions generate produces a usable plan on both

  Scenario: reassign partitions additional keeps the reassignment already running
    Then jvm acceptance reassign reassign execute: reassign partitions additional keeps the reassignment already running
