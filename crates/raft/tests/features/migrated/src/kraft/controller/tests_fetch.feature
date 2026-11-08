Feature: Kafka Fetch retry and observer progress

  Scenario Outline: Retry only refusals handled from the currently followed leader
    Given a replica follows node <leader> at epoch 5
    When node <sender> answers Fetch at epoch <epoch> with error <error>
    Then the replica immediately refetches <retry>

    Examples:
      | case                                          | leader | sender | epoch | error | retry |
      | NOT_LEADER_OR_FOLLOWER in the current epoch   | 1      | 1      | 5     | 6     | true  |
      | FENCED_LEADER_EPOCH in the current epoch      | 1      | 1      | 5     | 74    | true  |
      | UNKNOWN_LEADER_EPOCH                          | 1      | 1      | 5     | 75    | true  |
      | an answer from an older epoch                 | 1      | 1      | 4     | 0     | true  |
      | another refusal waits for the fetch timer     | 1      | 1      | 5     | 42    | false |
      | a refusal from a node that is not followed    | 1      | 3      | 5     | 6     | false |
      | a refusal while following no leader           | none   | 1      | 5     | 6     | false |

  Scenario: An observer's first fetch from offset zero earns no caught-up credit
    Given an observer has not fetched from the running single voter leader
    When observer 9 fetches zero after 0 milliseconds with log growth false
    Then the observer has fetched offset zero but has never caught up

  Scenario: An observer fetching the previous end of a growing log is credited at its previous fetch
    Given an observer has not fetched from the running single voter leader
    And observer 9 has fetched zero after 0 milliseconds with log growth false
    When observer 9 fetches previous-end after 20 milliseconds with log growth true
    Then the growing log credits the observer at its previous fetch time

  Scenario: An observer fetching the current log end is credited at its latest fetch
    Given an observer has not fetched from the running single voter leader
    And observer 9 has fetched zero after 0 milliseconds with log growth false
    And observer 9 has fetched previous-end after 20 milliseconds with log growth true
    When observer 9 fetches current-end after 20 milliseconds with log growth false
    Then fetching the current end credits the observer at its latest fetch time
