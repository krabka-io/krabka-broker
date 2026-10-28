Feature: Metadata-only bootstrap artifacts during formatting

  Scenario: Static bootstrap files belong only to a separate metadata directory
    Given log directories a,b with metadata directory m in static mode
    When the formatter writes all selected directories
    Then the selected directories contain exactly these format files
      | directory | files |
      | m | bootstrap.json,bootstrap.records.bin,meta.properties |
      | a | meta.properties |
      | b | meta.properties |

  Scenario: Dynamic bootstrap and the zero checkpoint belong only to the metadata directory
    Given log directories a,b with metadata directory m in standalone mode
    When the formatter writes all selected directories
    Then the selected directories contain exactly these format files
      | directory | files |
      | m | __cluster_metadata-0/00000000000000000000-0000000000.checkpoint,bootstrap.json,bootstrap.records.bin,meta.properties |
      | a | meta.properties |
      | b | meta.properties |

  Scenario: The first log directory is the default dynamic metadata directory
    Given log directories a,b with metadata directory first-log-dir in standalone mode
    When the formatter writes all selected directories
    Then the selected directories contain exactly these format files
      | directory | files |
      | a | __cluster_metadata-0/00000000000000000000-0000000000.checkpoint,bootstrap.json,bootstrap.records.bin,meta.properties |
      | b | meta.properties |

  Scenario: A metadata directory that is also a log directory is formatted once
    Given log directories a,b with metadata directory b in standalone mode
    When the formatter writes all selected directories
    Then the selected directories contain exactly these format files
      | directory | files |
      | a | meta.properties |
      | b | __cluster_metadata-0/00000000000000000000-0000000000.checkpoint,bootstrap.json,bootstrap.records.bin,meta.properties |
