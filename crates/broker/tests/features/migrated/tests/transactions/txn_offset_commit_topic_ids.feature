Feature: Transactional offset commits negotiate topic identities

  Scenario: send offsets to transaction commits by topic id
    Given a transaction-version-2 broker with input topic v6-in and a transactional producer
    When the producer enrolls offset 4 for generation 3 of missing group v6-gone and aborts
    Then the topic-ID offset request reports GROUP_ID_NOT_FOUND
    When the producer commits offset 7 for simple group v6-g through the same input topic
    And an administrator reads both consumer groups' committed offsets
    Then v6-g holds offset 7 for v6-in partition 0 and v6-gone holds no offsets
