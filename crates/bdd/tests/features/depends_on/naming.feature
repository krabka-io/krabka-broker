Feature: Topic names are checked before a topic is created

  Rule: A topic name is at most 249 characters

    Scenario: A name of 249 characters is accepted
      When a topic name of 249 characters is checked
      Then the name is accepted
