Feature: Kafka voter admission checks and replica catch-up
  Scenario: No fetch never catches up
    Given a quorum leader at kraft version 1 with pending reconfiguration false
    And candidate 4 has fetched with this history
      | directory | offset | append |
    When the controller adds voter 4 in directory 7
    Then voter admission returns not-caught-up
  Scenario: A short fetch is insufficient
    Given a quorum leader at kraft version 1 with pending reconfiguration false
    And candidate 4 has fetched with this history
      | directory | offset | append |
      | 7 | 0 | false |
    When the controller adds voter 4 in directory 7
    Then voter admission returns not-caught-up
  Scenario: Fetching the log end catches up
    Given a quorum leader at kraft version 1 with pending reconfiguration false
    And candidate 4 has fetched with this history
      | directory | offset | append |
      | 7 | end | false |
    When the controller adds voter 4 in directory 7
    Then voter admission returns committed
  Scenario: A replica keeping pace with appends catches up
    Given a quorum leader at kraft version 1 with pending reconfiguration false
    And candidate 4 has fetched with this history
      | directory | offset | append |
      | 7 | 0 | false |
      | 7 | previous-end | true |
    When the controller adds voter 4 in directory 7
    Then voter admission returns committed
  Scenario: Another directory cannot establish candidate progress
    Given a quorum leader at kraft version 1 with pending reconfiguration false
    And candidate 4 has fetched with this history
      | directory | offset | append |
      | 8 | end | false |
    When the controller adds voter 4 in directory 7
    Then voter admission returns not-caught-up
  Scenario: A stranger passes local checks without changing quorum state
    Given a quorum leader at kraft version 1 with pending reconfiguration false
    When the controller checks voter 4 in directory 7
    Then voter admission returns committed
    And checking admission leaves the log and voter set unchanged without a pending change
  Scenario Outline: Local admission refuses invalid requests in Kafka order
    Given a quorum leader at kraft version <version> with pending reconfiguration <pending>
    When the controller checks voter <id> in directory <directory>
    Then voter admission returns <outcome>
    Examples:
      | version | pending | id | directory | outcome |
      | 1 | false | 2 | 0 | duplicate |
      | 1 | false | 2 | 7 | duplicate |
      | 1 | true | 2 | 7 | pending-change |
      | 1 | true | 4 | 7 | pending-change |
      | 0 | false | 4 | 7 | unsupported-version |
