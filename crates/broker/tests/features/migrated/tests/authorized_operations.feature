Feature: Authorization masks are opt-in for each resource API
  Background:
    Given the anonymous plaintext principal is a configured super user

  Scenario Outline: Include flags control authorization discovery on each resource API
    Given resource <resource> is available for authorization discovery
    When <api> requests authorized operations <include> for that resource
    Then the reported authorized operations equal <expected>
    Examples:
      | resource | api | include | expected |
      | foo | Metadata | false | not-present |
      | foo | Metadata | true | full-topic-mask |
      | cluster | DescribeCluster | false | not-present |
      | cluster | DescribeCluster | true | full-cluster-mask |
      | g1 | DescribeGroups | false | not-present |
      | g1 | DescribeGroups | true | full-group-mask |

  Scenario: metadata cluster authorized operations super user gets full mask v9
    When a client pins Metadata to version 9 and requests cluster authorized operations
    Then Metadata v9 carries the full cluster authorized operations mask
