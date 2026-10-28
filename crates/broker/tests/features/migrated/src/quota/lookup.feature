Feature: Producer quota resolution follows Kafka user and client precedence
  A configured default entity can supply the rate while metric tags still bind
  the charged bucket to the actual user and client.

  Background:
    Given the cluster has no configured producer quotas

  Scenario: Named user-client pair overrides a named user with default client
    Given producer quota entity JSON [["user","alice"],["client-id","app"]] permits 1000 bytes per second
    And producer quota entity JSON [["user","alice"],["client-id",null]] permits 5000 bytes per second
    When the producer quota for user alice and client app is resolved
    Then the quota uses bucket JSON [["client-id","app"],["user","alice"]] at 1000 bytes per second

  Scenario: Named user with default client overrides user-only quota
    Given producer quota entity JSON [["user","alice"],["client-id",null]] permits 1000 bytes per second
    And producer quota entity JSON [["user","alice"]] permits 5000 bytes per second
    When the producer quota for user alice and client app is resolved
    Then the quota uses bucket JSON [["client-id","app"],["user","alice"]] at 1000 bytes per second

  Scenario: Named user overrides a default user with named client
    Given producer quota entity JSON [["user","alice"]] permits 1000 bytes per second
    And producer quota entity JSON [["user",null],["client-id","app"]] permits 5000 bytes per second
    When the producer quota for user alice and client app is resolved
    Then the quota uses bucket JSON [["user","alice"]] at 1000 bytes per second

  Scenario: Default user with named client overrides the default pair
    Given producer quota entity JSON [["user",null],["client-id","app"]] permits 1000 bytes per second
    And producer quota entity JSON [["user",null],["client-id",null]] permits 5000 bytes per second
    When the producer quota for user alice and client app is resolved
    Then the quota uses bucket JSON [["client-id","app"],["user","alice"]] at 1000 bytes per second

  Scenario: Default pair overrides default user-only quota
    Given producer quota entity JSON [["user",null],["client-id",null]] permits 1000 bytes per second
    And producer quota entity JSON [["user",null]] permits 5000 bytes per second
    When the producer quota for user alice and client app is resolved
    Then the quota uses bucket JSON [["client-id","app"],["user","alice"]] at 1000 bytes per second

  Scenario: Default user overrides a named client-only quota
    Given producer quota entity JSON [["user",null]] permits 1000 bytes per second
    And producer quota entity JSON [["client-id","app"]] permits 5000 bytes per second
    When the producer quota for user alice and client app is resolved
    Then the quota uses bucket JSON [["user","alice"]] at 1000 bytes per second

  Scenario: Named client overrides default client quota
    Given producer quota entity JSON [["client-id","app"]] permits 1000 bytes per second
    And producer quota entity JSON [["client-id",null]] permits 5000 bytes per second
    When the producer quota for user alice and client app is resolved
    Then the quota uses bucket JSON [["client-id","app"]] at 1000 bytes per second

  Scenario: Named user overrides the default user-client pair
    Given producer quota entity JSON [["user","alice"]] permits 1000 bytes per second
    And producer quota entity JSON [["user",null],["client-id",null]] permits 5000 bytes per second
    When the producer quota for user alice and client app is resolved
    Then the quota uses bucket JSON [["user","alice"]] at 1000 bytes per second

  Scenario: Named user with default client overrides default user with named client
    Given producer quota entity JSON [["user","alice"],["client-id",null]] permits 1000 bytes per second
    And producer quota entity JSON [["user",null],["client-id","app"]] permits 5000 bytes per second
    When the producer quota for user alice and client app is resolved
    Then the quota uses bucket JSON [["client-id","app"],["user","alice"]] at 1000 bytes per second

  Scenario: Named user quota overrides default user with a matching client
    Given producer quota entity JSON [["user","alice"]] permits 1000 bytes per second
    And producer quota entity JSON [["user",null],["client-id","app"]] permits 5000 bytes per second
    When the producer quota for user alice and client app is resolved
    Then the quota uses bucket JSON [["user","alice"]] at 1000 bytes per second

  Scenario: Default user overrides matching client-only quota
    Given producer quota entity JSON [["user",null]] permits 1000 bytes per second
    And producer quota entity JSON [["client-id","app"]] permits 5000 bytes per second
    When the producer quota for user alice and client app is resolved
    Then the quota uses bucket JSON [["user","alice"]] at 1000 bytes per second
