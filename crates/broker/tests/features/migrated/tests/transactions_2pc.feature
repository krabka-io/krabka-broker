Feature: Two-phase participation preserves Kafka coordinator semantics at transaction version 2

  Scenario: enable 2pc rejected when cluster disabled
    Given transaction.two.phase.commit.enable is false and unstable API versions are enabled
    When InitProducerId v6 enables 2PC for tid-2pc with keepPreparedTxn false
    Then InitProducerId returns error code 53

  Scenario: keep prepared txn without ongoing transaction is a noop
    Given transaction.two.phase.commit.enable is true and unstable API versions are enabled
    And transactional producer tid-keep is initialized without an ongoing transaction
    When InitProducerId v6 enables 2PC for tid-keep with keepPreparedTxn true
    Then InitProducerId returns error code 0
    And the response reports no ongoing producer id or epoch, both -1

  Scenario: enable 2pc persists no timeout sentinel
    Given transaction.two.phase.commit.enable is true and unstable API versions are enabled
    And transactional producer tid-2pc-ok is initialized without an ongoing transaction
    When InitProducerId v6 enables 2PC for tid-2pc-ok with keepPreparedTxn false
    Then InitProducerId returns error code 0
    And the assigned producer id is nonnegative
    When DescribeTransactions reads the persisted timeout for that transactional id
    Then the persisted timeout is the no-timeout sentinel 2147483647

  Scenario: two phase commit needs no transaction version 3 and 3 is refused
    Given transaction.two.phase.commit.enable is true and unstable API versions are enabled
    When ApiVersions reads the finalized transaction.version
    Then the finalized transaction.version is 2
    When UpdateFeatures tries to upgrade transaction.version to 3
    Then the full UpdateFeatures response is INVALID_UPDATE_VERSION 95 without feature rows and with message
      """
      The update failed for all features since the following feature had an error: Invalid update version 3 for feature transaction.version. Local controller 1 only supports versions 0-2
      """
