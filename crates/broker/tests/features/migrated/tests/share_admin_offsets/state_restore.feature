Feature: Share state persists terminal records and written delivery counts across recovery

  Scenario: delivery complete count restored across restart
    Given initialized share group g1 has 4 records on t
    When the recovery-test member acquires its available records
    Then the recovery-test acquisition contains 4 records
    When the member accepts offsets 1 through 3 and the delivery-complete count is persisted
    Then the share-state summary keeps start offset 0 and delivery-complete count 3
    When the share-state broker gracefully restarts and its summary is loaded
    Then the share-state summary keeps start offset 0 and delivery-complete count 3

  Scenario: an acquisition is durable only once a release writes it
    Given initialized share group g1 has 3 records on t
    When the recovery-test member acquires its available records
    Then the recovery-test acquisition contains 3 records
    When the share-state broker crashes and a new member reacquires after recovery
    Then the recovered acquisition is exactly offsets 0 through 2 at delivery count 1

  Scenario: a release persists the delivery count before a crash
    Given initialized share group g1 has 3 records on t
    When the recovery-test member acquires its available records
    Then the recovery-test acquisition contains 3 records
    When the recovery-test member releases offsets 0 through 2
    And the share-state broker crashes and a new member reacquires after recovery
    Then the recovered acquisition is exactly offsets 0 through 2 at delivery count 2
