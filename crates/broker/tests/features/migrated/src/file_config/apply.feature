Feature: Metadata log directory selection follows Kafka and command-line precedence

  Scenario: Omitting metadata directory uses the first data directory
    Given a log directory configuration file contains:
      """
      log_dir = "/data/a"
      extra_log_dirs = ["/data/b"]
      """
    When the file is applied and the metadata and data log paths are resolved
    Then the metadata log directory is /data/a and data log directories are JSON ["/data/a","/data/b"]

  Scenario: A separate metadata path is excluded from data directories
    Given a log directory configuration file contains:
      """
      log_dir = "/data/a"
      metadata_log_dir = "/meta"
      """
    When the file is applied and the metadata and data log paths are resolved
    Then the metadata log directory is /meta and data log directories are JSON ["/data/a"]

  Scenario: An existing data directory may also contain the metadata log
    Given a log directory configuration file contains:
      """
      log_dir = "/data/a"
      extra_log_dirs = ["/data/b"]
      metadata_log_dir = "/data/b"
      """
    When the file is applied and the metadata and data log paths are resolved
    Then the metadata log directory is /data/b and data log directories are JSON ["/data/a","/data/b"]

  Scenario: The command-line metadata path takes precedence over the file
    Given a log directory configuration file contains:
      """
      log_dir = "/data/a"
      metadata_log_dir = "/meta"
      """
    And the metadata-log-dir command-line flag selects /flag
    When the file is applied and the metadata and data log paths are resolved
    Then the metadata log directory is /flag and data log directories are JSON ["/data/a"]
