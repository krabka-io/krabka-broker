Feature: Verify-only partition checks report membership without enrolling partitions

  Scenario: tv2 verify only add partitions reports per partition codes
    Given a TV2 coordinator with producer verify-tid and two-partition topic t
    When the producer adds t partition 0 to its transaction
    Then the add response contains exactly t partition 0 with NONE
    When the producer verifies t partitions 0 and 1 without adding them
    Then verification reports NONE for added partition 0 and TRANSACTION_ABORTABLE for unregistered partition 1
