Feature: tests jvm acceptance quotas client quotas

  Scenario: jvm kafka configs alter client quota end to end
    Then jvm acceptance quotas client quotas: jvm kafka configs alter client quota end to end

  Scenario: jvm kafka configs alter ip quota end to end
    Then jvm acceptance quotas client quotas: jvm kafka configs alter ip quota end to end

  Scenario: jvm kafka configs alter controller mutation rate end to end
    Then jvm acceptance quotas client quotas: jvm kafka configs alter controller mutation rate end to end
