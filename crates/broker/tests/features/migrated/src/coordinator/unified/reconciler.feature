Feature: Subscription metadata changes advance consumer group epochs
  Scenario Outline: Topic growth and requested subscription changes independently trigger an epoch bump
    Given member m1 subscribes to t whose recorded partition count is <recorded> and current partition count is <current>
    When the coordinator refreshes subscription metadata with explicit epoch bump <requested>
    Then the epoch was bumped <bumped> to <epoch> and the current metadata hash is recorded without a pending refresh
    Examples:
      | recorded | current | requested | bumped | epoch |
      | 2 | 2 | false | false | 1 |
      | 2 | 2 | true | true | 2 |
      | 2 | 4 | false | true | 2 |
      | 2 | 4 | true | true | 2 |
