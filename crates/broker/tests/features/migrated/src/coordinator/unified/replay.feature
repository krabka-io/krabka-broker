Feature: Compacted consumer, Share and Streams logs rebuild their complete group state

  # Each value is the complete Kafka schema value including its version and tagged fields.
  # A tombstone has a null value. Rows are read in their literal persisted order.

  Scenario: Consumer replay of a member record creates the group and the member
    Given a fresh coordinator will recover consumer group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | consumer-member | m | 00000b6d2d696e7374616e6365077261636b2d61026d05686f73740208746f7069632d6109746f7069632d2e2a0000afc80672616e676500 |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "seed": {
          "assignment_timestamp_ms": 0,
          "current_per_member": {},
          "group_epoch": 1,
          "has_subscription_metadata_record": false,
          "members": {
            "m": {
              "classic": null,
              "client_host": "host",
              "client_id": "m",
              "instance_id": "m-instance",
              "rack_id": "rack-a",
              "rebalance_timeout_ms": 45000,
              "server_assignor": "range",
              "subscribed_topic_names": [
                "topic-a"
              ],
              "subscribed_topic_regex": "topic-.*"
            }
          },
          "metadata_hash": 0,
          "resolved_regexes": {},
          "target_epoch": 1,
          "target_per_member": {}
        }
      }
      """

  Scenario: Consumer replay of a current assignment creates an uninitialized member
    Given a fresh coordinator will recover consumer group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | consumer-current | m | 00000000000300000002000201010101010101010101010101010101030000000000000001000100 |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "seed": {
          "assignment_timestamp_ms": 0,
          "current_per_member": {
            "m": {
              "assigned_partitions": [
                {
                  "assignment_epochs": null,
                  "partitions": [
                    0,
                    1
                  ],
                  "topic_id": "01010101-0101-0101-0101-010101010101"
                }
              ],
              "member_epoch": 3,
              "partitions_pending_revocation": [],
              "previous_member_epoch": 2,
              "state": 0
            }
          },
          "group_epoch": 1,
          "has_subscription_metadata_record": false,
          "members": {
            "m": {
              "classic": null,
              "client_host": "",
              "client_id": "",
              "instance_id": null,
              "rack_id": null,
              "rebalance_timeout_ms": -1,
              "server_assignor": null,
              "subscribed_topic_names": [],
              "subscribed_topic_regex": null
            }
          },
          "metadata_hash": 0,
          "resolved_regexes": {},
          "target_epoch": 1,
          "target_per_member": {}
        }
      }
      """

  Scenario: Consumer replay of a target assignment creates the group but no member
    Given a fresh coordinator will recover consumer group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | consumer-target | m | 0000020303030303030303030303030303030302000000000000 |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "seed": {
          "assignment_timestamp_ms": 0,
          "current_per_member": {},
          "group_epoch": 1,
          "has_subscription_metadata_record": false,
          "members": {},
          "metadata_hash": 0,
          "resolved_regexes": {},
          "target_epoch": 1,
          "target_per_member": {
            "m": {
              "topic_partitions": [
                {
                  "partitions": [
                    0
                  ],
                  "topic_id": "03030303-0303-0303-0303-030303030303"
                }
              ]
            }
          }
        }
      }
      """

  Scenario: Consumer replay of a regular expression creates the group
    Given a fresh coordinator will recover consumer group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | consumer-regex | r | 00000202740000000000000001000000000000000200 |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "seed": {
          "assignment_timestamp_ms": 0,
          "current_per_member": {},
          "group_epoch": 1,
          "has_subscription_metadata_record": false,
          "members": {},
          "metadata_hash": 0,
          "resolved_regexes": {
            "r": {
              "timestamp_ms": 2,
              "topics": [
                "t"
              ],
              "version": 1
            }
          },
          "target_epoch": 1,
          "target_per_member": {}
        }
      }
      """

  Scenario: Consumer replay of an epoch replaces the last one, lower or not
    Given a fresh coordinator will recover consumer group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | consumer-group-metadata |  | 0000000000050100080000000000000009 |
      | consumer-group-metadata |  | 0000000000020100080000000000000009 |
      | consumer-target-epoch |  | 0000000000000100080000000000000004 |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "seed": {
          "assignment_timestamp_ms": 4,
          "current_per_member": {},
          "group_epoch": 2,
          "has_subscription_metadata_record": false,
          "members": {},
          "metadata_hash": 9,
          "resolved_regexes": {},
          "target_epoch": 0,
          "target_per_member": {}
        }
      }
      """

  Scenario: Consumer replay of Kafka's deletion order removes the whole group
    Given a fresh coordinator will recover consumer group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | consumer-group-metadata |  | 0000000000050100080000000000000009 |
      | consumer-member | m | 00000b6d2d696e7374616e6365077261636b2d61026d05686f73740208746f7069632d6109746f7069632d2e2a0000afc80672616e676500 |
      | consumer-target-epoch |  | 0000000000050100080000000000000004 |
      | consumer-target | m | 0000020303030303030303030303030303030302000000000000 |
      | consumer-current | m | 00000000000500000004000201010101010101010101010101010101030000000000000001000100 |
      | consumer-regex | r | 00000202740000000000000001000000000000000200 |
      | consumer-current | m | tombstone |
      | consumer-target | m | tombstone |
      | consumer-target-epoch |  | tombstone |
      | consumer-member | m | tombstone |
      | consumer-regex | r | tombstone |
      | consumer-subscription-metadata |  | tombstone |
      | consumer-group-metadata |  | tombstone |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "seed": null
      }
      """

  Scenario: Consumer replay of tombstones of a group the log does not hold are ignored
    Given a fresh coordinator will recover consumer group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | consumer-current | m | tombstone |
      | consumer-target | m | tombstone |
      | consumer-target-epoch |  | tombstone |
      | consumer-member | m | tombstone |
      | consumer-regex | r | tombstone |
      | consumer-subscription-metadata |  | tombstone |
      | consumer-group-metadata |  | tombstone |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "seed": null
      }
      """

  Scenario: Consumer replay of the tombstone of a current assignment leaves the member at epoch -1
    Given a fresh coordinator will recover consumer group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | consumer-member | m | 00000b6d2d696e7374616e6365077261636b2d61026d05686f73740208746f7069632d6109746f7069632d2e2a0000afc80672616e676500 |
      | consumer-current | m | 00000000000300000002000201010101010101010101010101010101030000000000000001000100 |
      | consumer-current | m | tombstone |
      | consumer-target | other | tombstone |
      | consumer-member | other | tombstone |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "seed": {
          "assignment_timestamp_ms": 0,
          "current_per_member": {
            "m": {
              "assigned_partitions": [],
              "member_epoch": -1,
              "partitions_pending_revocation": [],
              "previous_member_epoch": -1,
              "state": 0
            }
          },
          "group_epoch": 1,
          "has_subscription_metadata_record": false,
          "members": {
            "m": {
              "classic": null,
              "client_host": "host",
              "client_id": "m",
              "instance_id": "m-instance",
              "rack_id": "rack-a",
              "rebalance_timeout_ms": 45000,
              "server_assignor": "range",
              "subscribed_topic_names": [
                "topic-a"
              ],
              "subscribed_topic_regex": "topic-.*"
            }
          },
          "metadata_hash": 0,
          "resolved_regexes": {},
          "target_epoch": 1,
          "target_per_member": {}
        }
      }
      """

  Scenario: Consumer replay of a member tombstone before its current assignment tombstone
    Given a fresh coordinator will recover consumer group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | consumer-member | m | 00000b6d2d696e7374616e6365077261636b2d61026d05686f73740208746f7069632d6109746f7069632d2e2a0000afc80672616e676500 |
      | consumer-current | m | 00000000000300000002000201010101010101010101010101010101030000000000000001000100 |
      | consumer-member | m | tombstone |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "error": "Received a tombstone record to delete member m but did not receive ConsumerGroupCurrentMemberAssignmentValue tombstone."
      }
      """

  Scenario: Consumer replay of a member tombstone of a member that never had a current assignment
    Given a fresh coordinator will recover consumer group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | consumer-member | m | 00000b6d2d696e7374616e6365077261636b2d61026d05686f73740208746f7069632d6109746f7069632d2e2a0000afc80672616e676500 |
      | consumer-member | m | tombstone |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "error": "Received a tombstone record to delete member m but did not receive ConsumerGroupCurrentMemberAssignmentValue tombstone."
      }
      """

  Scenario: Consumer replay of a member tombstone while the member has a target
    Given a fresh coordinator will recover consumer group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | consumer-member | m | 00000b6d2d696e7374616e6365077261636b2d61026d05686f73740208746f7069632d6109746f7069632d2e2a0000afc80672616e676500 |
      | consumer-target | m | 0000020303030303030303030303030303030302000000000000 |
      | consumer-current | m | tombstone |
      | consumer-member | m | tombstone |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "error": "Received a tombstone record to delete member m but did not receive ConsumerGroupTargetAssignmentMetadataValue tombstone."
      }
      """

  Scenario: Consumer replay of a target metadata tombstone while a member has a target
    Given a fresh coordinator will recover consumer group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | consumer-target | m | 0000020303030303030303030303030303030302000000000000 |
      | consumer-target-epoch |  | tombstone |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "error": "Received a tombstone record to delete target assignment of g but the assignment still has 1 members."
      }
      """

  Scenario: Consumer replay of a group tombstone with a member left
    Given a fresh coordinator will recover consumer group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | consumer-member | m | 00000b6d2d696e7374616e6365077261636b2d61026d05686f73740208746f7069632d6109746f7069632d2e2a0000afc80672616e676500 |
      | consumer-target-epoch |  | tombstone |
      | consumer-group-metadata |  | tombstone |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "error": "Received a tombstone record to delete group g but the group still has 1 members."
      }
      """

  Scenario: Consumer replay of a group tombstone before the target metadata tombstone
    Given a fresh coordinator will recover consumer group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | consumer-group-metadata |  | 0000000000030100080000000000000009 |
      | consumer-group-metadata |  | tombstone |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "error": "Received a tombstone record to delete group g but did not receive ConsumerGroupTargetAssignmentMetadataValue tombstone."
      }
      """

  Scenario: Consumer replay of an assignment epoch below -1
    Given a fresh coordinator will recover consumer group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | consumer-target-epoch |  | 0000fffffffe0100080000000000000004 |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "error": "The assignment epoch must be non-negative or -1."
      }
      """

  Scenario: Consumer replay of a consumer record for a share group
    Given a fresh coordinator will recover consumer group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | share-group-metadata |  | 000000000001000000000000000000 |
      | consumer-member | m | 00000b6d2d696e7374616e6365077261636b2d61026d05686f73740208746f7069632d6109746f7069632d2e2a0000afc80672616e676500 |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "error": "Group g is not a consumer group"
      }
      """

  Scenario: Consumer replay of a consumer tombstone for a share group
    Given a fresh coordinator will recover consumer group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | share-group-metadata |  | 000000000001000000000000000000 |
      | consumer-member | m | tombstone |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "error": "Group g is not a consumer group"
      }
      """

  Scenario: Share replay of every record of a group
    Given a fresh coordinator will recover share group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | share-group-metadata |  | 000000000015000000000000000900 |
      | share-member | m | 0000077261636b2d62026d05686f7374020c73686172652d746f70696300 |
      | share-target-epoch |  | 0000000000160100080000000000000004 |
      | share-target | m | 000002040404040404040404040404040404040300000000000000030000 |
      | share-current | m | 0000000000060000000500020404040404040404040404040404040402000000010000 |
      | share-state-partitions |  | 0000010200000000000000000000000000000001076f7264657273030000000000000001000100 |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "seed": {
          "assignment_timestamp_ms": 4,
          "current_per_member": {
            "m": {
              "assigned_partitions": [
                {
                  "partitions": [
                    1
                  ],
                  "topic_id": "04040404-0404-0404-0404-040404040404"
                }
              ],
              "member_epoch": 6,
              "previous_member_epoch": 5
            }
          },
          "group_epoch": 21,
          "members": {
            "m": {
              "client_host": "host",
              "client_id": "m",
              "rack_id": "rack-b",
              "subscribed_topic_names": [
                "share-topic"
              ]
            }
          },
          "metadata_hash": 9,
          "state_partition_metadata": {
            "deleting": [],
            "initialized": [
              {
                "partitions": [
                  0,
                  1
                ],
                "topic_id": "00000000-0000-0000-0000-000000000001",
                "topic_name": "orders"
              }
            ],
            "initializing": []
          },
          "target_epoch": 22,
          "target_per_member": {
            "m": {
              "topic_partitions": [
                {
                  "partitions": [
                    0,
                    3
                  ],
                  "topic_id": "04040404-0404-0404-0404-040404040404"
                }
              ]
            }
          }
        }
      }
      """

  Scenario: Share replay of a current assignment creates the group and an uninitialized member
    Given a fresh coordinator will recover share group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | share-current | m | 0000000000060000000500020404040404040404040404040404040402000000010000 |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "seed": {
          "assignment_timestamp_ms": 0,
          "current_per_member": {
            "m": {
              "assigned_partitions": [
                {
                  "partitions": [
                    1
                  ],
                  "topic_id": "04040404-0404-0404-0404-040404040404"
                }
              ],
              "member_epoch": 6,
              "previous_member_epoch": 5
            }
          },
          "group_epoch": 1,
          "members": {
            "m": {
              "client_host": "",
              "client_id": "",
              "rack_id": null,
              "subscribed_topic_names": []
            }
          },
          "metadata_hash": 0,
          "state_partition_metadata": {
            "deleting": [],
            "initialized": [],
            "initializing": []
          },
          "target_epoch": 1,
          "target_per_member": {}
        }
      }
      """

  Scenario: Share replay of the state partition metadata creates the group
    Given a fresh coordinator will recover share group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | share-state-partitions |  | 0000010200000000000000000000000000000001076f7264657273030000000000000001000100 |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "seed": {
          "assignment_timestamp_ms": 0,
          "current_per_member": {},
          "group_epoch": 1,
          "members": {},
          "metadata_hash": 0,
          "state_partition_metadata": {
            "deleting": [],
            "initialized": [
              {
                "partitions": [
                  0,
                  1
                ],
                "topic_id": "00000000-0000-0000-0000-000000000001",
                "topic_name": "orders"
              }
            ],
            "initializing": []
          },
          "target_epoch": 1,
          "target_per_member": {}
        }
      }
      """

  Scenario: Share replay of Kafka's deletion order removes the whole group
    Given a fresh coordinator will recover share group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | share-group-metadata |  | 000000000002000000000000000900 |
      | share-member | m | 0000077261636b2d62026d05686f7374020c73686172652d746f70696300 |
      | share-target-epoch |  | 0000000000020100080000000000000004 |
      | share-target | m | 000002040404040404040404040404040404040300000000000000030000 |
      | share-current | m | 0000000000020000000100020404040404040404040404040404040402000000010000 |
      | share-state-partitions |  | 0000010200000000000000000000000000000001076f7264657273030000000000000001000100 |
      | share-current | m | tombstone |
      | share-target | m | tombstone |
      | share-target-epoch |  | tombstone |
      | share-member | m | tombstone |
      | share-state-partitions |  | tombstone |
      | share-group-metadata |  | tombstone |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "seed": null
      }
      """

  Scenario: Share replay of tombstones of a group the log does not hold are ignored
    Given a fresh coordinator will recover share group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | share-current | m | tombstone |
      | share-target | m | tombstone |
      | share-target-epoch |  | tombstone |
      | share-member | m | tombstone |
      | share-state-partitions |  | tombstone |
      | share-group-metadata |  | tombstone |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "seed": null
      }
      """

  Scenario: Share replay of a member tombstone before its current assignment tombstone
    Given a fresh coordinator will recover share group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | share-member | m | 0000077261636b2d62026d05686f7374020c73686172652d746f70696300 |
      | share-member | m | tombstone |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "error": "Received a tombstone record to delete member m with invalid leave group epoch."
      }
      """

  Scenario: Share replay of a member tombstone while the member has a target
    Given a fresh coordinator will recover share group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | share-member | m | 0000077261636b2d62026d05686f7374020c73686172652d746f70696300 |
      | share-target | m | 000002040404040404040404040404040404040300000000000000030000 |
      | share-current | m | tombstone |
      | share-member | m | tombstone |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "error": "Received a tombstone record to delete member m but member exists in target assignment."
      }
      """

  Scenario: Share replay of a group tombstone while a target is left
    Given a fresh coordinator will recover share group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | share-target | m | 000002040404040404040404040404040404040300000000000000030000 |
      | share-group-metadata |  | tombstone |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "error": "Received a tombstone record to delete group g but the target assignment still has 1 members."
      }
      """

  Scenario: Share replay of a group tombstone before the target metadata tombstone
    Given a fresh coordinator will recover share group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | share-group-metadata |  | 000000000002000000000000000900 |
      | share-group-metadata |  | tombstone |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "error": "Received a tombstone record to delete group g but target assignment epoch in invalid."
      }
      """

  Scenario: Share replay of a share record for a consumer group
    Given a fresh coordinator will recover share group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | consumer-group-metadata |  | 00000000000100 |
      | share-group-metadata |  | 000000000002000000000000000900 |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "error": "Group g is not a share group."
      }
      """

  Scenario: Streams replay of every record of a group
    Given a fresh coordinator will recover streams group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | streams-group-metadata |  | 00000000001e000000000000002c0200040000001f011902156e756d2e7374616e6462792e7265706c69636173023100 |
      | streams-member | m | 00000b6d2d696e7374616e6365077261636b2d63026d05686f737400007530000000040870726f63657373010a6c6f63616c686f73741f900002046170700873747265616d730000 |
      | streams-topology |  | 00000000001f0100 |
      | streams-target-epoch |  | 0000000000200100080000000000000004 |
      | streams-target | m | 000002023003000000000000000100010100 |
      | streams-current | m | 0000000000070000000602020230030000000000000001010009030000000700000007010101010100 |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "seed": {
          "assignment_epoch": 32,
          "assignment_timestamp_ms": 4,
          "current_per_member": {
            "m": {
              "active": {
                "0": [
                  0,
                  1
                ]
              },
              "active_epochs": {
                "0": [
                  7,
                  7
                ]
              },
              "active_pending_revocation": {},
              "active_pending_revocation_epochs": {},
              "member_epoch": 7,
              "previous_member_epoch": 6,
              "standby": {},
              "standby_pending_revocation": {},
              "state": 2,
              "warmup": {},
              "warmup_pending_revocation": {}
            }
          },
          "description_epochs": {
            "failed": -1,
            "stored": -1
          },
          "group_epoch": 30,
          "last_assignment_configs": {
            "num.standby.replicas": "1"
          },
          "members": {
            "m": {
              "client_host": "host",
              "client_id": "m",
              "client_tags": [
                [
                  "app",
                  "streams"
                ]
              ],
              "instance_id": "m-instance",
              "process_id": "process",
              "rack_id": "rack-c",
              "rebalance_timeout_ms": 30000,
              "topology_epoch": 4,
              "user_endpoint": {
                "host": "localhost",
                "port": 8080
              }
            }
          },
          "metadata_hash": 44,
          "target_per_member": {
            "m": {
              "active": {
                "0": [
                  0,
                  1
                ]
              },
              "standby": {},
              "warmup": {}
            }
          },
          "topology": {
            "epoch": 31,
            "subtopologies": []
          },
          "validated_topology_epoch": 31
        }
      }
      """

  Scenario: Streams replay of a current assignment creates the group and an uninitialized member
    Given a fresh coordinator will recover streams group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | streams-current | m | 0000000000070000000602020230030000000000000001010009030000000700000007010101010100 |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "seed": {
          "assignment_epoch": 1,
          "assignment_timestamp_ms": 0,
          "current_per_member": {
            "m": {
              "active": {
                "0": [
                  0,
                  1
                ]
              },
              "active_epochs": {
                "0": [
                  7,
                  7
                ]
              },
              "active_pending_revocation": {},
              "active_pending_revocation_epochs": {},
              "member_epoch": 7,
              "previous_member_epoch": 6,
              "standby": {},
              "standby_pending_revocation": {},
              "state": 2,
              "warmup": {},
              "warmup_pending_revocation": {}
            }
          },
          "description_epochs": {
            "failed": -1,
            "stored": -1
          },
          "group_epoch": 1,
          "last_assignment_configs": {},
          "members": {
            "m": {
              "client_host": "",
              "client_id": "",
              "client_tags": [],
              "instance_id": null,
              "process_id": "",
              "rack_id": null,
              "rebalance_timeout_ms": -1,
              "topology_epoch": -1,
              "user_endpoint": null
            }
          },
          "metadata_hash": 0,
          "target_per_member": {},
          "topology": null,
          "validated_topology_epoch": 0
        }
      }
      """

  Scenario: Streams replay of a topology creates the group, and its tombstone removes the topology
    Given a fresh coordinator will recover streams group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | streams-topology |  | 00000000001f0100 |
      | streams-topology |  | tombstone |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "seed": {
          "assignment_epoch": 1,
          "assignment_timestamp_ms": 0,
          "current_per_member": {},
          "description_epochs": {
            "failed": -1,
            "stored": -1
          },
          "group_epoch": 1,
          "last_assignment_configs": {},
          "members": {},
          "metadata_hash": 0,
          "target_per_member": {},
          "topology": null,
          "validated_topology_epoch": 0
        }
      }
      """

  Scenario: Streams replay of Kafka's deletion order removes the whole group
    Given a fresh coordinator will recover streams group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | streams-group-metadata |  | 00000000001e000000000000002c0200040000001f011902156e756d2e7374616e6462792e7265706c69636173023100 |
      | streams-member | m | 00000b6d2d696e7374616e6365077261636b2d63026d05686f737400007530000000040870726f63657373010a6c6f63616c686f73741f900002046170700873747265616d730000 |
      | streams-topology |  | 00000000001f0100 |
      | streams-target-epoch |  | 0000000000200100080000000000000004 |
      | streams-target | m | 000002023003000000000000000100010100 |
      | streams-current | m | 0000000000070000000602020230030000000000000001010009030000000700000007010101010100 |
      | streams-current | m | tombstone |
      | streams-target | m | tombstone |
      | streams-target-epoch |  | tombstone |
      | streams-member | m | tombstone |
      | streams-group-metadata |  | tombstone |
      | streams-topology |  | tombstone |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "seed": null
      }
      """

  Scenario: Streams replay of tombstones of a group the log does not hold are ignored
    Given a fresh coordinator will recover streams group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | streams-current | m | tombstone |
      | streams-target | m | tombstone |
      | streams-target-epoch |  | tombstone |
      | streams-member | m | tombstone |
      | streams-group-metadata |  | tombstone |
      | streams-topology |  | tombstone |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "seed": null
      }
      """

  Scenario: Streams replay of the tombstone of a current assignment leaves the member at epoch -1
    Given a fresh coordinator will recover streams group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | streams-member | m | 00000b6d2d696e7374616e6365077261636b2d63026d05686f737400007530000000040870726f63657373010a6c6f63616c686f73741f900002046170700873747265616d730000 |
      | streams-current | m | 0000000000070000000602020230030000000000000001010009030000000700000007010101010100 |
      | streams-current | m | tombstone |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "seed": {
          "assignment_epoch": 1,
          "assignment_timestamp_ms": 0,
          "current_per_member": {
            "m": {
              "active": {},
              "active_epochs": {},
              "active_pending_revocation": {},
              "active_pending_revocation_epochs": {},
              "member_epoch": -1,
              "previous_member_epoch": -1,
              "standby": {},
              "standby_pending_revocation": {},
              "state": 2,
              "warmup": {},
              "warmup_pending_revocation": {}
            }
          },
          "description_epochs": {
            "failed": -1,
            "stored": -1
          },
          "group_epoch": 1,
          "last_assignment_configs": {},
          "members": {
            "m": {
              "client_host": "host",
              "client_id": "m",
              "client_tags": [
                [
                  "app",
                  "streams"
                ]
              ],
              "instance_id": "m-instance",
              "process_id": "process",
              "rack_id": "rack-c",
              "rebalance_timeout_ms": 30000,
              "topology_epoch": 4,
              "user_endpoint": {
                "host": "localhost",
                "port": 8080
              }
            }
          },
          "metadata_hash": 0,
          "target_per_member": {},
          "topology": null,
          "validated_topology_epoch": 0
        }
      }
      """

  Scenario: Streams replay of a member tombstone before its current assignment tombstone
    Given a fresh coordinator will recover streams group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | streams-member | m | 00000b6d2d696e7374616e6365077261636b2d63026d05686f737400007530000000040870726f63657373010a6c6f63616c686f73741f900002046170700873747265616d730000 |
      | streams-member | m | tombstone |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "error": "Received a tombstone record to delete member m but did not receive StreamsGroupCurrentMemberAssignmentValue tombstone."
      }
      """

  Scenario: Streams replay of a group tombstone with a member left
    Given a fresh coordinator will recover streams group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | streams-member | m | 00000b6d2d696e7374616e6365077261636b2d63026d05686f737400007530000000040870726f63657373010a6c6f63616c686f73741f900002046170700873747265616d730000 |
      | streams-target-epoch |  | tombstone |
      | streams-group-metadata |  | tombstone |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "error": "Received a tombstone record to delete group g but the group still has 1 members."
      }
      """

  Scenario: Streams replay of a group tombstone with a target left but no member: the streams check does not look at targets
    Given a fresh coordinator will recover streams group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | streams-target | m | 000002023003000000000000000100010100 |
      | streams-target | m | tombstone |
      | streams-target-epoch |  | tombstone |
      | streams-target | m | 000002023003000000000000000100010100 |
      | streams-group-metadata |  | tombstone |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "seed": null
      }
      """

  Scenario: Streams replay of a group tombstone before the target metadata tombstone
    Given a fresh coordinator will recover streams group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | streams-group-metadata |  | 000000000002000000000000002c0100040000001f |
      | streams-group-metadata |  | tombstone |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "error": "Received a tombstone record to delete group g but did not receive StreamsGroupTargetAssignmentMetadataValue tombstone."
      }
      """

  Scenario: Streams replay of a configuration list that names a key twice
    Given a fresh coordinator will recover streams group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | streams-group-metadata |  | 000000000002000000000000002c0200040000001f010b0302610231000261023200 |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "error": "Duplicate key a (attempted merging values 1 and 2)"
      }
      """

  Scenario: Streams replay of a streams record for a consumer group
    Given a fresh coordinator will recover streams group g from these compacted offsets records
      | family | member_or_regex | value_hex |
      | consumer-group-metadata |  | 00000000000100 |
      | streams-topology |  | 00000000001f0100 |
    When bootstrap reads the persisted group records in order
    Then the complete recovered seed or exact startup error is as follows and successful replay has the same cached seed
      """
      {
        "error": "Group g is not a streams group."
      }
      """
