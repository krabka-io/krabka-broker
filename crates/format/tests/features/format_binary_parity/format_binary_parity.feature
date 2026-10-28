Feature: Kafka-compatible format binary output and recovery
  Scenario Outline: Cluster identity arguments are normalized in files and stdout
    Given format cluster ID argument <input>
    When the format binary executes
    Then the cluster ID is stored and printed as <canonical>

    Examples:
      | input                                | canonical              |
      | AQIDBAUGBwgJCgsMDQ4PEA                | AQIDBAUGBwgJCgsMDQ4PEA |
      | 01020304-0506-0708-090a-0b0c0d0e0f10 | AQIDBAUGBwgJCgsMDQ4PEA |
      | 01020304-0506-0708-090A-0B0C0D0E0F10 | AQIDBAUGBwgJCgsMDQ4PEA |
      | AQIDBAUGBwgJCgsMDQ4PEA==              | AQIDBAUGBwgJCgsMDQ4PEA |
      | AQIDBAUGBwgJCgsMDQ4PEB                | AQIDBAUGBwgJCgsMDQ4PEA |
      | AAAAAAAAAAAAAAAAAAAAAA                | AAAAAAAAAAAAAAAAAAAAAA |
      | AAAAAAAAAAAAAAAAAAAAAQ                | AAAAAAAAAAAAAAAAAAAAAQ |

  Scenario Outline: Invalid cluster IDs are refused before creating files
    Given format cluster ID argument <input>
    When the format binary executes
    Then the malformed cluster ID exits with usage error before writing a directory

    Examples:
      | input                                |
      | AQIDBAUGBwgJCgsMDQ4P                 |
      | AQIDBAUGBwgJCgsMDQ4PEBES             |
      | AQIDBAUGBwgJCgsMDQ4P+A               |
      | 01020304-0506-0708-090a-0b0c0d0e0fzz |
      | 0102030405060708090a0b0c0d0e0f10     |

  Scenario Outline: Directory IDs and voter IDs are stored in Kafka form
    Given format directory identity options <options>
    When the format binary executes
    Then the broker reads directory UUID 100 in Kafka form

    Examples:
      | options |
      | --directory-id AAAAAAAAAAAAAAAAAAAAZA |
      | --directory-id 00000000-0000-0000-0000-000000000064 |
      | --initial-controllers 1@controller-1:9093:AAAAAAAAAAAAAAAAAAAAZA |
      | --initial-controllers 1@controller-1:9093:00000000-0000-0000-0000-000000000064 |

  Scenario: Repeated log flags format every directory
    Given a standalone format of log directories meta,data using repeated flags and metadata first-log-dir
    And a reference standalone bootstrap record count
    When the format binary executes
    Then the binary reports and writes these directories in metadata-first order
      | directory | kind |
      | meta | metadata-voter |
      | data | data |

  Scenario: Comma-separated log flags format every directory
    Given a standalone format of log directories meta,data using comma-separated flags and metadata first-log-dir
    And a reference standalone bootstrap record count
    When the format binary executes
    Then the binary reports and writes these directories in metadata-first order
      | directory | kind |
      | meta | metadata-voter |
      | data | data |

  Scenario: A separate metadata log directory is written before every data directory
    Given a standalone format of log directories a,b using comma-separated flags and metadata m
    And a reference standalone bootstrap record count
    When the format binary executes
    Then the binary reports and writes these directories in metadata-first order
      | directory | kind |
      | m | metadata-voter |
      | a | data |
      | b | data |

  Scenario: A metadata directory also listed among log directories is written only once
    Given a standalone format of log directories a,b using comma-separated flags and metadata b
    And a reference standalone bootstrap record count
    When the format binary executes
    Then the binary reports and writes these directories in metadata-first order
      | directory | kind |
      | b | metadata-voter |
      | a | data |

  Scenario: Ignoring formatted directories preserves identity and makes reruns idempotent
    Given directory a is formatted and directory b is absent
    When the format binary executes
    Then the existing directory refuses the run and directory b remains absent
    When the format binary tries another cluster with ignore-formatted
    Then the existing cluster ID mismatch is refused even when formatted directories are ignored
    When the format binary ignores formatted directories and discovers the cluster ID
    Then directory a keeps its identity and directory b joins the cluster with a distinct identity
    When the format binary ignores the fully formatted directory set again
    Then the already-formatted message is printed and every file remains unchanged

  Scenario Outline: An interruption before marker publication can be resumed
    Given a standalone format of log directories meta,data using comma-separated flags and metadata first-log-dir
    When the format binary is interrupted after <artifact>
    Then the injected interruption exits with code 4
    When the format binary is rerun without interruption
    Then the format binary exits with code 0
    And both interrupted-format directories contain complete Kafka artifacts with voters only in the checkpoint

    Examples:
      | artifact |
      | 00000000000000000000-0000000000.checkpoint |
      | bootstrap.records.bin |
      | bootstrap.json |
      | meta.properties.tmp |

  Scenario: A published marker requires explicit skipping when resuming an interrupted run
    Given a standalone format of log directories meta,data using comma-separated flags and metadata first-log-dir
    When the format binary is interrupted after meta.properties
    Then the injected interruption exits with code 4
    When the format binary is rerun without interruption
    Then the format binary exits with code 3
    When the interrupted format is resumed with ignore-formatted
    Then the format binary exits with code 0
    And both interrupted-format directories contain complete Kafka artifacts with voters only in the checkpoint

  Scenario: Written meta.properties matches Kafka's ordered property representation
    Given metadata directory meta and log directory data for node 2147483647 with directory UUID 100
    When the format binary executes
    Then every meta.properties file has Kafka's ordered fields and UTC date comment
