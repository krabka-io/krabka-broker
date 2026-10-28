@docker
Feature: Acknowledgements from every replica make writes durable across brokers
  Scenario: All replica acknowledgements make records visible to a committed consumer on another broker
    Given three brokers host a topic replicated on all three nodes
    When the stock producer writes a hundred records with acknowledgements from all replicas
    Then the replicated producer command succeeds
    When a read committed stock consumer reads from the third broker after replication settles
    Then the committed consumer receives at least a hundred records
