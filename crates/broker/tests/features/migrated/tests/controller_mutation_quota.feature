Feature: tests controller mutation quota

  Scenario: controller mutation rate throttles create topics
    Then controller mutation quota: controller mutation rate throttles create topics

  Scenario: unthrottled create topics unaffected
    Then controller mutation quota: unthrottled create topics unaffected

  Scenario: controller mutation rate throttles delete topics
    Then controller mutation quota: controller mutation rate throttles delete topics
