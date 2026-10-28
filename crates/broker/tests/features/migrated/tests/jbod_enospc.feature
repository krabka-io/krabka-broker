Feature: tests jbod enospc

  Scenario: prometheus value parser ignores help and other metrics
    Given Prometheus returns this exposition text
      """
      # HELP krabka_broker_offline_log_dirs count
      another_metric 9
      krabka_broker_offline_log_dirs 1
      """
    When the parser looks up metric krabka_broker_offline_log_dirs
    Then the metric value is 1
    When the parser looks up metric missing
    Then no metric value is returned

  Scenario: real enospc moves leadership and preserves acked records across restart
    Given three packaged brokers host twelve RF3 partitions and node 1 has a 32-MiB primary and 16-MiB extra tmpfs, each containing a node-1-led partition with ISR 3
    When up to 128 fill records of 256 KiB fill the extra-directory partition, recording only acknowledged keys
    Then node 1 reports one offline log directory, its extra directory returns KAFKA_STORAGE_ERROR 56 and its primary remains online with code 0
    When the primary-directory partition receives a sixteen-byte healthy-primary record
    Then the healthy-primary record is acknowledged while only the extra directory is offline
    When up to 256 primary-fill records of 256 KiB exhaust the primary directory, recording only acknowledged keys
    Then node 1 exits with all directories offline and both exhausted partitions elect a leader other than 1
    And every acknowledged key from both affected partitions remains readable
    When both node 1 filesystems are freed and grown to 64 MiB and its broker restarts on existing state
    Then node 1 reports zero offline directories, both directories host healthy replicas and both affected ISRs return to 3
    And every acknowledged key from both affected partitions remains readable
