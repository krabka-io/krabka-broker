Feature: Transactional appends verify coordinator state and preserve log boundaries

  Scenario: a transactional produce needs a verified transaction
    Given transactional producer v10-partition-not-added has partition state unregistered after 0 additional initializations
    When Produce version 10 sends a batch with transaction ID true, transactional flag true, epoch delta 0, and sequence 0
    Then the complete Produce response and log delta match
      | error | base_offset | log_start | log_append_time | message                                    | append_delta |
      | 48    | -1          | -1        | -1              | Partition was not added to the transaction | 0            |

  Scenario: v11 verifies unregistered partitions without appending
    Given transactional producer v11-partition-not-added has partition state unregistered after 0 additional initializations
    When Produce version 11 sends a batch with transaction ID true, transactional flag true, epoch delta 0, and sequence 0
    Then the complete Produce response and log delta match
      | error | base_offset | log_start | log_append_time | message | append_delta |
      | 120   | -1          | -1        | -1              | absent  | 0            |

  Scenario: v11 accepts a registered partition
    Given transactional producer v11-partition-added has partition state registered after 0 additional initializations
    When Produce version 11 sends a batch with transaction ID true, transactional flag true, epoch delta 0, and sequence 0
    Then the complete Produce response and log delta match
      | error | base_offset      | log_start | log_append_time | message | append_delta |
      | 0     | previous_log_end | 0         | -1              | absent  | 1            |

  Scenario: v12 automatically adds an unregistered partition
    Given transactional producer v12-partition-not-added has partition state unregistered after 0 additional initializations
    When Produce version 12 sends a batch with transaction ID true, transactional flag true, epoch delta 0, and sequence 0
    Then the complete Produce response and log delta match
      | error | base_offset      | log_start | log_append_time | message | append_delta |
      | 0     | previous_log_end | 0         | -1              | absent  | 1            |

  Scenario: a missing request transaction ID is unauthorized
    Given transactional producer v11-no-transactional-id has partition state registered after 0 additional initializations
    When Produce version 11 sends a batch with transaction ID false, transactional flag true, epoch delta 0, and sequence 0
    Then the complete Produce response and log delta match
      | error | base_offset | log_start | log_append_time | message | append_delta |
      | 53    | -1          | -1        | -1              | absent  | 0            |

  Scenario: an open transaction rejects a stale producer epoch
    Given transactional producer open-transaction-stale-epoch has partition state open after 1 additional initializations
    When Produce version 11 sends a batch with transaction ID true, transactional flag true, epoch delta -1, and sequence 1
    Then the complete Produce response and log delta match
      | error | base_offset | log_start | log_append_time | message | append_delta |
      | 47    | -1          | -1        | -1              | absent  | 0            |

  Scenario: an open transaction rejects an ordinary batch at the log
    Given transactional producer open-transaction-non-transactional-batch has partition state open after 0 additional initializations
    When Produce version 11 sends a batch with transaction ID true, transactional flag false, epoch delta 0, and sequence 1
    Then the complete Produce response and log delta match
      | error | base_offset | log_start | log_append_time | message | append_delta |
      | 48    | -1          | 0         | -1              | absent  | 0            |

  Scenario: a committed transaction rejects replay at the old epoch
    Given transactional producer committed-replay-of-the-last-batch has partition state committed after 0 additional initializations
    When Produce version 11 sends a batch with transaction ID true, transactional flag true, epoch delta 0, and sequence 0
    Then the complete Produce response and log delta match
      | error | base_offset | log_start | log_append_time | message | append_delta |
      | 47    | -1          | -1        | -1              | absent  | 0            |
