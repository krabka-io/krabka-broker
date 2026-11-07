Feature: Topics are created with a checked name

  Depends on: Topic naming - naming.feature

  Rule: A topic with an accepted name can be created

    Scenario: A topic with a name of 250 characters is refused
      When a topic name of 250 characters is checked
      Then the name is refused
