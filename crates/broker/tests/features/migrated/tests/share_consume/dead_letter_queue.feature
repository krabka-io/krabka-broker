Feature: Share dead-letter writes preserve causes and allow source consumption to advance

  Scenario: a rejected batch is dead lettered then archived
    Given an acquired DLQ batch has 3 records at share version 2 with message limit unlimited
    When the member rejects source offsets 0 through 2
    And the consumer waits for 3 dead-letter records
    Then the source share start offset reaches 3
    And all three rejected records retain their source values and all six DLQ headers
    And the DLQ meters report 3 written records, 1 produce attempts, and 0 failures

  Scenario: rejects that are not neighbours are all dead lettered and counted
    Given an acquired DLQ batch has 5 records at share version 2 with message limit unlimited
    When the member rejects the disjoint source offsets zero, two, and four
    And the consumer waits for 3 dead-letter records
    Then the disjoint DLQ records retain exactly offsets zero, two, and four with rejection metadata
    And the DLQ meters report 3 written records, 3 produce attempts, and 0 failures

  Scenario: a write the topic refuses counts as failed and still archives
    Given an acquired DLQ batch has 1 records at share version 2 with message limit 100
    When the member rejects source offsets 0 through 0
    Then the source share start offset reaches 1
    And the DLQ meters report 0 written records, 1 produce attempts, and 1 failures
    And the DLQ is empty

  Scenario: a record that uses up its deliveries is dead lettered
    Given one acquired DLQ record has a 150-millisecond lock and a two-delivery limit
    When the first DLQ acquisition expires and the member fetches its second delivery
    Then the DLQ source is acquired once at delivery count two
    When the consumer waits for 1 dead-letter records
    Then the source share start offset reaches 1
    And the delivery-limited DLQ record preserves v0 and threshold metadata at delivery count two

  Scenario: a reject below share version two writes nothing
    Given an acquired DLQ batch has 2 records at share version 1 with message limit unlimited
    When the member rejects source offsets 0 through 1
    Then the source share start offset reaches 2
    When 500 milliseconds elapse for an intentionally disabled DLQ write
    Then the DLQ is empty

  Scenario: a failed write still archives the record
    Given the DLQ topic is absent, automatic creation is false, and 2 source records are acquired
    When the member rejects source offsets 0 through 1
    Then the source share start offset reaches 2
    And the missing DLQ topic has not been created
    And the DLQ meters report 0 written records, 0 produce attempts, and 0 failures

  Scenario: a missing topic is created when the cluster allows it
    Given the DLQ topic is absent, automatic creation is true, and 1 source records are acquired
    When the member rejects source offsets 0 through 0
    And the automatically created DLQ topic appears in metadata
    And the consumer waits for 1 dead-letter records
    Then the source share start offset reaches 1
    And the created DLQ contains v0 and opts into group dead-letter writes

  Scenario: a copied record too big for the topic fails and still archives
    Given one acquired 1000-byte source record targets a DLQ topic with a 1100-byte message limit
    When the member rejects source offsets 0 through 0
    Then the source share start offset reaches 1
    And the DLQ meters report 0 written records, 1 produce attempts, and 1 failures
    And the DLQ is empty

  Scenario: records that only the remote tier holds are acquired and dead lettered
    Given three source records have left local disk and are acquired from the remote tier
    Then the remote-tier acquisition contains all three original v0,v1,v2 values
    When the member rejects source offsets 0 through 2
    And the consumer waits for 3 dead-letter records
    Then the remote-source DLQ contains all three original v0,v1,v2 values
