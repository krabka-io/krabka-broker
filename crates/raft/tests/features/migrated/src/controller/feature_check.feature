Feature: Recovered controller feature compatibility
  A controller refuses to apply a finalized level outside its supported range
  with Kafka's exact refusal. Unknown features support only disabled level zero.

  Scenario: The latest production metadata version starts without unstable support
    Given unstable feature versions are enabled: false
    And the recovered metadata image finalizes these feature levels:
      | name | level |
      | metadata.version | 30 |
    When the controller checks recovered feature compatibility
    Then every recovered feature level is supported

  Scenario: An unstable metadata level is refused when its flag is disabled
    Given unstable feature versions are enabled: false
    And the recovered metadata image finalizes these feature levels:
      | name | level |
      | metadata.version | 33 |
    When the controller checks recovered feature compatibility
    Then feature metadata.version at level 33 is refused with Kafka supported range 7-30

  Scenario: An unstable metadata level starts with its flag enabled
    Given unstable feature versions are enabled: true
    And the recovered metadata image finalizes these feature levels:
      | name | level |
      | metadata.version | 33 |
    When the controller checks recovered feature compatibility
    Then every recovered feature level is supported

  Scenario: A known feature above its supported range is refused
    Given unstable feature versions are enabled: true
    And the recovered metadata image finalizes these feature levels:
      | name | level |
      | share.version | 5 |
    When the controller checks recovered feature compatibility
    Then feature share.version at level 5 is refused with Kafka supported range 0-2

  Scenario: A metadata level below the supported range is refused
    Given unstable feature versions are enabled: true
    And the recovered metadata image finalizes these feature levels:
      | name | level |
      | metadata.version | 3 |
    When the controller checks recovered feature compatibility
    Then feature metadata.version at level 3 is refused with Kafka supported range 7-34

  Scenario: The first unsupported finalized feature determines the refusal
    Given unstable feature versions are enabled: false
    And the recovered metadata image finalizes these feature levels:
      | name | level |
      | metadata.version | 33 |
      | share.version | 5 |
    When the controller checks recovered feature compatibility
    Then feature metadata.version at level 33 is refused with Kafka supported range 7-30

  Scenario: An unknown feature supports only disabled level zero
    Given unstable feature versions are enabled: false
    And the recovered metadata image finalizes these feature levels:
      | name | level |
      | some.future.feature | 9 |
    When the controller checks recovered feature compatibility
    Then feature some.future.feature at level 9 is refused with Kafka supported range 0

  Scenario: The unstable flag does not enable unknown features
    Given unstable feature versions are enabled: true
    And the recovered metadata image finalizes these feature levels:
      | name | level |
      | some.future.feature | 1 |
    When the controller checks recovered feature compatibility
    Then feature some.future.feature at level 1 is refused with Kafka supported range 0

  Scenario: A supported known feature does not conceal an unknown feature
    Given unstable feature versions are enabled: false
    And the recovered metadata image finalizes these feature levels:
      | name | level |
      | metadata.version | 30 |
      | some.future.feature | 2 |
    When the controller checks recovered feature compatibility
    Then feature some.future.feature at level 2 is refused with Kafka supported range 0

  Scenario: An image without finalized features is compatible
    Given unstable feature versions are enabled: false
    And the recovered metadata image finalizes these feature levels:
      | name | level |
    When the controller checks recovered feature compatibility
    Then every recovered feature level is supported
