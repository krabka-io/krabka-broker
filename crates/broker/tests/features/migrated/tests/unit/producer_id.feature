Feature: Kafka producer identity allocation
  Scenario: init producer id returns fresh pid
    Given a single broker with no transaction coordinator topic
    When a client requests an idempotent producer identity
    Then the fresh idempotent identity has producer id zero and epoch zero

  Scenario: init producer id without coordinator bootstrap returns not coordinator
    Given a single broker with no transaction coordinator topic
    When a client initializes transactional producer tx-1
    Then producer initialization returns NOT_COORDINATOR

  Scenario: find coordinator txn creates topic then returns local broker
    Given a single broker with no transaction coordinator topic
    When the client first looks up the transaction coordinator for my-tid
    Then the transaction coordinator is initially unavailable with no broker address
    When the client retries transaction coordinator discovery until it is ready
    Then the transaction coordinator names this broker at its bound listener

  Scenario: init producer id with transactional id returns real pid
    Given a single broker with no transaction coordinator topic
    And the transaction coordinator for my-tid is ready
    When a client initializes transactional producer my-tid
    Then the transaction coordinator allocates a real producer id at epoch zero

  Scenario: init producer id with same tid bumps epoch
    Given a single broker with no transaction coordinator topic
    And the transaction coordinator for stable-tid is ready
    When a client initializes transactional producer stable-tid
    Then the first transactional identity allocation succeeds
    When a client initializes transactional producer stable-tid
    Then the same transaction id keeps its producer id and increments the epoch
