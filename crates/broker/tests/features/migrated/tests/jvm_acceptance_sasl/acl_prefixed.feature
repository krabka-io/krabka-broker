@docker
Feature: Prefix ACLs authorize only matching resources
  Scenario: A prefix read grant permits only matching topic names
    Given alice can read the team prefix and her group while both matching and unrelated topics contain a seed record
    When alice consumes from the topic covered by her prefix grant
    Then alice receives the matching topic's seeded record
    When alice consumes from the unrelated topic using the same authorized group
    Then the unrelated topic is refused with TopicAuthorizationException
