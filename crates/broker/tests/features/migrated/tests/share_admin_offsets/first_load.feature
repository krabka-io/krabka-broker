Feature: Latest share offsets resolve per leader and become durable on the first acknowledgement

  Scenario: an unwritten start is resolved again and the first ack persists it
    Given a latest-reset share group g1 whose topic t contains three records
    When the first leader loads the latest share partition
    Then the first load stores its leader epoch while start offset and described lag remain uninitialized
    When two more records arrive and the broker restarts under a newer partition leader epoch
    And the new leader loads latest again without acknowledging records
    Then the new load still leaves the persisted start offset uninitialized
    When two further records arrive and the reloaded member fetches
    Then only offsets 5 and 6 are acquired at delivery count one
    When the reloaded member accepts offset 6 and waits for its delivery-complete record
    Then the newer epoch durably reports start 5, one completed record, and lag one
