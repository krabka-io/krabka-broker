Feature: Stateful streams topologies provision their internal changelog topics

  Scenario: stateful member triggers internal topic creation
    Given a streams-enabled broker holds topic sk-input with 1 partitions for group streams-app-2
    And the stateful streams topology declares app-store-changelog with derived partition count
    When the stateful member joins and retries heartbeats while internal topics are provisioned
    Then the stateful member owns task zero without MISSING_INTERNAL_TOPICS
    And the controller image contains app-store-changelog with one partition
