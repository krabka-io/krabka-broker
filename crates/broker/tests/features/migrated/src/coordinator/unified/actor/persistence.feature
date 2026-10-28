Feature: Persist consumer member subscriptions using Kafka normalization
  Scenario: Named subscriptions sort their topics and preserve the empty regex convention
    Given member m1 subscribes by name to b a and c without a regex
    When the coordinator builds the member subscription log record
    Then the persisted names are sorted a b c and the absent regex is an explicit empty string
