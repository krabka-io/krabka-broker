Feature: Kafka Fetch retry and observer progress
  @refetch
  Scenario Outline: Retry only refusals handled from the currently followed leader
    Given a replica follows node <leader> at epoch 5
    When node <sender> answers Fetch at epoch <epoch> with error <error>
    Then the replica immediately refetches <retry>
    Examples:
      | leader | sender | epoch | error | retry |
      | 1 | 1 | 5 | 6 | true |
      | 1 | 1 | 5 | 74 | true |
      | 1 | 1 | 5 | 75 | true |
      | 1 | 1 | 4 | 0 | true |
      | 1 | 1 | 5 | 42 | false |
      | 1 | 3 | 5 | 6 | false |
      | none | 1 | 5 | 6 | false |
  @observer
  Scenario: An observer keeping pace with appends earns Kafka caught-up credit
    Given an observer has not fetched from the running single voter leader
    When observer 9 fetches zero after 0 milliseconds with log growth false
    Then the observer has fetched offset zero but has never caught up
    When observer 9 fetches previous-end after 20 milliseconds with log growth true
    Then the growing log credits the observer at its previous fetch time
    When observer 9 fetches current-end after 20 milliseconds with log growth false
    Then fetching the current end credits the observer at its latest fetch time
