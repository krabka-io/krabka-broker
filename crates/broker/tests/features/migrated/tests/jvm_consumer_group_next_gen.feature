@docker
Feature: Stock classic and consumer protocol clients coordinate through the unified group service
  Scenario: A consumer protocol member reads stock producer records
    Given a broker with bidirectional group migration hosts topic kip848-rt with 1 partitions
    When the classic producer publishes a b and c with a ten second metadata block limit
    Then the classic producer completes successfully
    When a consumer protocol console consumer in group g-rt reads 3 records
    Then the console consumer emits values a,b,c

  Scenario: A consumer protocol group can be described by stock tooling
    Given a broker with bidirectional group migration hosts topic kip848-d with 1 partitions
    When the classic producer publishes values 1,2
    And a consumer protocol console consumer in group g-d reads 2 records
    And the stock consumer group tool describes group g-d
    Then the description contains group g-d

  Scenario: An inactive consumer protocol group can be deleted
    Given a broker with bidirectional group migration hosts topic kip848-del with 1 partitions
    When the classic producer publishes values x
    And a consumer protocol console consumer in group g-del reads 1 records
    And the stock consumer group tool deletes group g del
    Then the consumer group deletion succeeds

  Scenario: Classic and consumer protocol groups independently consume the same topic
    Given a broker with bidirectional group migration hosts topic kip848-coex with 1 partitions
    When the classic producer publishes values p,q
    And a classic protocol console consumer in group g-classic reads 2 records
    Then the console consumer emits values p,q
    When a consumer protocol console consumer in group g-next reads 2 records
    Then the console consumer emits values p,q

  Scenario: A group migrates in place from classic to the consumer protocol
    Given a broker with bidirectional group migration hosts topic mig with 4 partitions
    And a deterministic keyed migration batch contains
      | key | value |
      | 0 | a |
      | 4 | b |
      | 5 | c |
      | 1 | d |
      | 0 | e |
      | 4 | f |
      | 5 | g |
      | 1 | h |
    When the deterministic keyed migration batch is produced
    Then the classic producer completes successfully
    When a sole classic protocol member drains eight records from group g migrate and prints assignments
    Then the member covers every partition zero through three
    When the deterministic keyed migration batch is produced
    Then the classic producer completes successfully
    When a sole consumer protocol member drains eight records from group g migrate and prints assignments
    Then the member covers every partition zero through three
    When the stock consumer group tool describes group g-migrate
    Then the migrated group description includes its subscribed topic
