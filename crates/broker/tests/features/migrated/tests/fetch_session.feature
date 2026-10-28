Feature: tests fetch session

  Scenario: new session then incremental filters unchanged partitions
    Then fetch session: new session then incremental filters unchanged partitions

  Scenario: forgotten topics drop partitions from subscription
    Then fetch session: forgotten topics drop partitions from subscription

  Scenario: unknown session id returns not found
    Then fetch session: unknown session id returns not found

  Scenario: stale session epoch returns invalid epoch
    Then fetch session: stale session epoch returns invalid epoch

  Scenario: close session drops cache entry
    Then fetch session: close session drops cache entry

  Scenario: sessionless zero id with stray epoch is session id not found
    Then fetch session: sessionless zero id with stray epoch is session id not found

  Scenario: sessionless full fetch round trip
    Then fetch session: sessionless full fetch round trip
