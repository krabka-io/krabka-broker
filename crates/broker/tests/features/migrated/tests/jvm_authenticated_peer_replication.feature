@docker
Feature: Stock clients write and replicate through authenticated broker peers
  Scenario: Stock clients write through PLAIN authenticated peers
    Given two registered PLAIN authenticated peers host a single replica topic for the administrator
    When the authenticated stock producer writes fifty numbered records through the peer cluster
    Then the peer cluster producer succeeds
    And the partition leader stores all fifty records

  Scenario: Stock clients replicate records through SASL SSL peers and controllers
    Given two registered SASL SSL peers with encrypted controller traffic host an alice SCRAM topic replicated on both brokers
    When the authenticated stock producer writes fifty numbered records through the peer cluster
    Then the peer cluster producer succeeds
    And both encrypted replicas store all fifty records
