Feature: Startup recovers interrupted replica moves and removes orphan future directories

  Scenario: Restart resumes a planted future directory for an existing partition
    Given a stopped two-directory broker stores t partition 0 with records v0 through v4 and an empty t-0-future directory exists in the other log directory
    When the broker starts against both log directories in rejoin mode
    Then the interrupted move completes in the target directory, the source has no t partitions and t-0-future is renamed away

  Scenario: Startup removes future directories for topics that never existed
    Given ghost was never created but ghost-0-future exists in the extra configured log directory
    When the broker starts against both log directories in fresh-bootstrap mode
    Then startup removes ghost-0-future and DescribeLogDirs reports no future entries
