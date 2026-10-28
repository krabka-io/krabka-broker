Feature: Share acknowledgements accept, release, or archive acquired records

  Scenario: consume accept restart
    Given an acknowledgement-test member has 3 available records on t
    When the acknowledgement-test member opens its session and acquires records
    Then the acknowledgement fetch contains 3 records at delivery count 1
    When the member acknowledges through offset 2 with accept
    And the acknowledgement-test member fetches at epoch 2
    Then the acknowledgement fetch acquires no records
    When the accepted start offset 3 is persisted and the broker restarts
    And the acknowledgement-test member fetches at epoch 0
    Then the acknowledgement fetch acquires no records

  Scenario: release redelivers
    Given an acknowledgement-test member has 2 available records on t
    When the acknowledgement-test member opens its session and acquires records
    Then the acknowledgement fetch contains 2 records at delivery count 1
    When the member acknowledges through offset 1 with release
    And the acknowledgement-test member fetches at epoch 2
    Then the acknowledgement fetch contains 2 records at delivery count 2

  Scenario: reject archives
    Given an acknowledgement-test member has 2 available records on t
    When the acknowledgement-test member opens its session and acquires records
    Then the acknowledgement fetch contains 2 records at delivery count 1
    When the member acknowledges through offset 1 with reject
    And the acknowledgement-test member fetches at epoch 2
    Then the acknowledgement fetch acquires no records
    When one new record is appended after the rejected pair and fetched from epoch three
    Then only the new offset two is acquired after the rejected records
