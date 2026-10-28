Feature: External broker feature compatibility and log directory identity

  Scenario Outline: Finalized feature levels constrain broker registration
    Given the cluster has finalized metadata.version 25 and group.version 1
    And the registering broker advertises feature ranges <ranges>
    When registration checks feature compatibility with finalized levels
    Then registration validation returns error code <code>

    Examples:
      | ranges | code |
      | metadata.version:7:25;group.version:0:1 | 0 |
      | metadata.version:7:24;group.version:0:1 | 35 |
      | metadata.version:7:25;group.version:0:1;share.version:1:1 | 35 |
      | metadata.version:7:25;group.version:0:1;share.version:0:1 | 0 |
      | group.version:0:1 | 119 |
      | none | 119 |
      | metadata.version:7:25 | 35 |
      | group.version:2:3 | 35 |

  Scenario Outline: Directory IDs must be fresh or already owned by this broker
    Given broker 1 owns directory 500 and broker 2 owns directory 600 at epoch 10
    And broker 2 requests directory IDs <directories>
    When registration checks directory ownership and reserved IDs
    Then registration validation returns error code <code>

    Examples:
      | directories | code |
      | 1000 | 0 |
      | 1000,1001 | 0 |
      | none | 119 |
      | 0 | 119 |
      | 99 | 119 |
      | 100 | 0 |
      | 1000,1000 | 119 |
      | 1000,500 | 119 |
      | 600 | 0 |
