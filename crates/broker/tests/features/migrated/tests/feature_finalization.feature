Feature: Finalize supported feature levels and validate updates without persisting them
  Scenario: an unstable metadata version is refused by default
    Given unstable feature versions are enabled false
    When metadata.version is upgraded to the trunk maximum with validate only false
    Then the update fails with error 95 no result rows and a message naming feature metadata.version
    And the metadata refusal exactly names the requested trunk level and controller supported range 7-30
    When the client reads API versions and feature levels
    Then metadata supported levels remain 7 through the latest production version

  Scenario: finalizes metadata version and surfaces in api versions
    Given unstable feature versions are enabled true
    When metadata.version is upgraded to the trunk maximum with validate only false
    Then the update succeeds for feature metadata.version
    When the client reads API versions and feature levels
    Then the finalized metadata level is the trunk maximum with a nonnegative epoch

  Scenario: share version two is refused by default
    Given unstable feature versions are enabled false
    When the client reads API versions and feature levels
    Then share.version supported maximum is 1 and finalized level is 1
    When feature share.version is upgraded to level 2 with validate only false
    Then the update fails with error 95 no result rows and a message naming feature share.version
    And the share refusal names level 2 and controller supported range 0-1

  Scenario: share version two is finalizable under unstable feature versions
    Given unstable feature versions are enabled true
    When the client reads API versions and feature levels
    Then share.version supported maximum is 2 and finalized level is 1
    When feature share.version is upgraded to level 2 with validate only true
    Then the update succeeds for feature share.version
    When the client reads API versions and feature levels
    Then share.version supported maximum is 2 and finalized level is 1
    When feature share.version is upgraded to level 2 with validate only false
    Then the update succeeds for feature share.version
    When the client reads API versions and feature levels
    Then share.version supported maximum is 2 and finalized level is 2
    When feature share.version is upgraded to level 3 with validate only false
    Then the update fails with error 95 no result rows and a message naming feature share.version

  Scenario Outline: unsupported feature names and out of range levels are refused
    Given unstable feature versions are enabled false
    When feature <feature> is upgraded to level <level> with validate only false
    Then the update fails with error 95 no result rows and a message naming feature <feature>
    Examples:
      | feature | level |
      | not.a.feature | 1 |
      | metadata.version | 99 |
      | metadata.version | 6 |

  Scenario: validate only does not persist
    Given unstable feature versions are enabled true
    When the client records baseline API versions and feature levels
    Then baseline metadata is at the latest production level with a nonnegative epoch
    When metadata.version is upgraded to the trunk maximum with validate only true
    Then the update succeeds for feature metadata.version
    When the client reads API versions and feature levels
    Then validation leaves the production metadata level and finalized epoch unchanged
