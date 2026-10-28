Feature: Applying topic configuration through the admin protocol

  Scenario: alter configs round trip
    Given an admin client connected to one standalone broker
    And topic t-alter exists with one local replica
    When AlterConfigs sets retention.ms to 60000
    Then AlterConfigs accepts the topic configuration
    And the local partition adopts a 60000 millisecond retention within 10 seconds

  Scenario: alter configs rejects unknown key
    Given an admin client connected to one standalone broker
    And topic t-bad-cfg exists with one local replica
    When AlterConfigs sets not.a.topic.config to 1000
    Then AlterConfigs rejects unknown key not.a.topic.config with INVALID_CONFIG and names it in the error

  Scenario: min insync replicas is clamped to the replica count for acks all
    Given an admin client connected to one standalone broker
    And topic t-min-isr exists with one local replica
    And min.insync.replicas is 2 on that single-replica topic
    When one record is produced with acknowledgement mode all
    Then the produce request succeeds despite the configured minimum exceeding the replica count
    When one record is produced with acknowledgement mode leader
    Then the produce request succeeds despite the configured minimum exceeding the replica count
