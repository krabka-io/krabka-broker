Feature: Startup authentication and admin mutations leave durable redacted audit evidence
  Scenario: audit topic exists after startup
    Given a fresh broker with its audit partition initialized
    When a client requests Metadata for the audit topic
    Then the audit topic is listed without error and has at least one partition

  Scenario: broker started event is written to audit topic
    Given a fresh broker with its audit partition initialized
    When a client fetches the durable startup audit record
    Then the audit topic contains a BrokerStarted activity of class 6002

  Scenario: every admin mutation is audited
    Given a fresh broker with its audit partition initialized
    And admin mutation inputs name topic audited-orders and user audited-user
    When the admin creates the audit example topic
    Then the CreateTopics admin mutation succeeds
    When the admin grows the audit example topic to two partitions
    Then the CreatePartitions admin mutation succeeds
    When the admin sets retention to 604800000
    Then the AlterConfigs admin mutation succeeds
    When the admin incrementally sets maximum message bytes to 1048576
    Then the IncrementalAlterConfigs admin mutation succeeds
    When the admin replaces all overrides with cleanup policy delete
    Then the AlterConfigs replacement admin mutation succeeds
    When the admin sets the audit user producer byte quota to 1048576
    Then the AlterClientQuotas admin mutation succeeds
    When the admin provisions audit user SCRAM SHA 256 with salt audited-salt and salted password audited-salted-password-32-byte
    Then the AlterUserScramCredentials admin mutation succeeds
    Given the audit example topic has two records eligible for trimming
    When the admin trims the topic before offset 2 and retries the same trim
    Then the DeleteRecords admin mutation succeeds
    And the DeleteRecords retry admin mutation succeeds
    When the admin deletes the audit example topic
    Then the DeleteTopics admin mutation succeeds
    And each successful admin mutation has audit evidence naming its changed resource:
      | operation | resource |
      | CreateTopics | audited-orders |
      | CreatePartitions | audited-orders |
      | AlterConfigs | audited-orders |
      | IncrementalAlterConfigs | audited-orders |
      | AlterClientQuotas | user=audited-user |
      | AlterUserScramCredentials | audited-user |
      | DeleteRecords | audited-orders-0 |
      | DeleteTopics | audited-orders |
    And the replacement audit row names cleanup policy and both removed override keys
    When the client consumes the full audit topic after all mutations
    Then the trim is audited once and secrets are redacted while changed config keys remain visible

  Scenario: sasl plain logins are audited either way
    Given a SASL audit broker accepts alice password alice-secret
    When alice authenticates successfully to read the audit topic
    And another alice connection uses the wrong password and requests Metadata
    Then the wrong password connection serves no request
    And both successful and refused PLAIN logins name User alice and the loopback peer
