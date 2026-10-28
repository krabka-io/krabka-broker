Feature: tests share consume dead letter queue

  Scenario: a rejected batch is dead lettered then archived
    Then share consume dead letter queue: a rejected batch is dead lettered then archived

  Scenario: rejects that are not neighbours are all dead lettered and counted
    Then share consume dead letter queue: rejects that are not neighbours are all dead lettered and counted

  Scenario: a write the topic refuses counts as failed and still archives
    Then share consume dead letter queue: a write the topic refuses counts as failed and still archives

  Scenario: a record that uses up its deliveries is dead lettered
    Then share consume dead letter queue: a record that uses up its deliveries is dead lettered

  Scenario: a reject below share version two writes nothing
    Then share consume dead letter queue: a reject below share version two writes nothing

  Scenario: a failed write still archives the record
    Then share consume dead letter queue: a failed write still archives the record

  Scenario: a missing topic is created when the cluster allows it
    Then share consume dead letter queue: a missing topic is created when the cluster allows it

  Scenario: a copied record too big for the topic is written with headers alone
    Then share consume dead letter queue: a copied record too big for the topic is written with headers alone

  Scenario: records that only the remote tier holds are acquired and dead lettered
    Then share consume dead letter queue: records that only the remote tier holds are acquired and dead lettered
