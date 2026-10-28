Feature: Consumer heartbeat identity and classic group migration

  @consumer-identity
  Scenario: A static member releases its instance with epoch -2
    Given consumer group g has group and target epochs 5 with four partitions of topic t UUID 09090909-0909-0909-0909-090909090909 and these settled members
      | member | instance | epoch | previous_epoch | partitions |
      | m1 | null | 5 | 4 | [0,1] |
      | s1 | "i1" | 5 | 4 | [2,3] |
    When that consumer heartbeats subscribed to t with rebalance timeout 60000 ms and these identity fields
      | member | instance | epoch | owned |
      | s1 | "i1" | -2 | null |
    Then the complete consumer response and remaining member epochs and instance owner are
      """
      {
        "response": {
          "error_code": 0,
          "error_message": null,
          "member_id": "s1",
          "member_epoch": -2,
          "heartbeat_interval_ms": 0,
          "assignment": null
        },
        "members": [
          [
            "m1",
            5
          ],
          [
            "s1",
            -2
          ]
        ],
        "instance_i1": "s1"
      }
      """

  @consumer-identity
  Scenario: A new member takes a released static instance
    Given consumer group g has group and target epochs 5 with four partitions of topic t UUID 09090909-0909-0909-0909-090909090909 and these settled members
      | member | instance | epoch | previous_epoch | partitions |
      | m1 | null | 5 | 4 | [0,1] |
      | s1 | "i1" | 5 | 4 | [2,3] |
    And static instance i1 has already released member s1 with epoch -2
    When that consumer heartbeats subscribed to t with rebalance timeout 60000 ms and these identity fields
      | member | instance | epoch | owned |
      | s2 | "i1" | 0 | [] |
    Then the complete consumer response and remaining member epochs and instance owner are
      """
      {
        "response": {
          "error_code": 0,
          "error_message": null,
          "member_id": "s2",
          "member_epoch": 5,
          "heartbeat_interval_ms": 5000,
          "assignment": [
            2,
            3
          ]
        },
        "members": [
          [
            "m1",
            5
          ],
          [
            "s2",
            5
          ]
        ],
        "instance_i1": "s2"
      }
      """

  @consumer-identity
  Scenario: An unreleased static instance refuses a new member
    Given consumer group g has group and target epochs 5 with four partitions of topic t UUID 09090909-0909-0909-0909-090909090909 and these settled members
      | member | instance | epoch | previous_epoch | partitions |
      | m1 | null | 5 | 4 | [0,1] |
      | s1 | "i1" | 5 | 4 | [2,3] |
    When that consumer heartbeats subscribed to t with rebalance timeout 60000 ms and these identity fields
      | member | instance | epoch | owned |
      | s2 | "i1" | 0 | [] |
    Then the complete consumer response and remaining member epochs and instance owner are
      """
      {
        "response": {
          "error_code": 111,
          "error_message": "Static member s2 with instance id i1 cannot join the group because the instance id is owned by s1 member.",
          "member_id": null,
          "member_epoch": 0,
          "heartbeat_interval_ms": 0,
          "assignment": null
        },
        "members": [
          [
            "m1",
            5
          ],
          [
            "s1",
            5
          ]
        ],
        "instance_i1": "s1"
      }
      """

  @consumer-identity
  Scenario: A known dynamic member rejoins at epoch zero
    Given consumer group g has group and target epochs 5 with four partitions of topic t UUID 09090909-0909-0909-0909-090909090909 and these settled members
      | member | instance | epoch | previous_epoch | partitions |
      | m1 | null | 5 | 4 | [0,1] |
      | s1 | "i1" | 5 | 4 | [2,3] |
    When that consumer heartbeats subscribed to t with rebalance timeout 60000 ms and these identity fields
      | member | instance | epoch | owned |
      | m1 | null | 0 | [] |
    Then the complete consumer response and remaining member epochs and instance owner are
      """
      {
        "response": {
          "error_code": 0,
          "error_message": null,
          "member_id": "m1",
          "member_epoch": 5,
          "heartbeat_interval_ms": 5000,
          "assignment": [
            0,
            1
          ]
        },
        "members": [
          [
            "m1",
            5
          ],
          [
            "s1",
            5
          ]
        ],
        "instance_i1": "s1"
      }
      """

  @consumer-identity
  Scenario: A previous epoch may acknowledge a subset of the current assignment
    Given consumer group g has group and target epochs 5 with four partitions of topic t UUID 09090909-0909-0909-0909-090909090909 and these settled members
      | member | instance | epoch | previous_epoch | partitions |
      | m1 | null | 5 | 4 | [0,1] |
      | s1 | "i1" | 5 | 4 | [2,3] |
    When that consumer heartbeats subscribed to t with rebalance timeout 60000 ms and these identity fields
      | member | instance | epoch | owned |
      | m1 | null | 4 | [0] |
    Then the complete consumer response and remaining member epochs and instance owner are
      """
      {
        "response": {
          "error_code": 0,
          "error_message": null,
          "member_id": "m1",
          "member_epoch": 5,
          "heartbeat_interval_ms": 5000,
          "assignment": [
            0,
            1
          ]
        },
        "members": [
          [
            "m1",
            5
          ],
          [
            "s1",
            5
          ]
        ],
        "instance_i1": "s1"
      }
      """

  @consumer-identity
  Scenario: A previous epoch cannot retain a superset of the assignment
    Given consumer group g has group and target epochs 5 with four partitions of topic t UUID 09090909-0909-0909-0909-090909090909 and these settled members
      | member | instance | epoch | previous_epoch | partitions |
      | m1 | null | 5 | 4 | [0,1] |
      | s1 | "i1" | 5 | 4 | [2,3] |
    When that consumer heartbeats subscribed to t with rebalance timeout 60000 ms and these identity fields
      | member | instance | epoch | owned |
      | m1 | null | 4 | [0, 1, 2] |
    Then the complete consumer response and remaining member epochs and instance owner are
      """
      {
        "response": {
          "error_code": 110,
          "error_message": "The consumer group member has a smaller member epoch (4) than the one known by the group coordinator (5). The member must abandon all its partitions and rejoin.",
          "member_id": null,
          "member_epoch": 0,
          "heartbeat_interval_ms": 0,
          "assignment": null
        },
        "members": [
          [
            "m1",
            5
          ],
          [
            "s1",
            5
          ]
        ],
        "instance_i1": "s1"
      }
      """

  @consumer-identity
  Scenario: An epoch older than the previous epoch is fenced
    Given consumer group g has group and target epochs 5 with four partitions of topic t UUID 09090909-0909-0909-0909-090909090909 and these settled members
      | member | instance | epoch | previous_epoch | partitions |
      | m1 | null | 5 | 4 | [0,1] |
      | s1 | "i1" | 5 | 4 | [2,3] |
    When that consumer heartbeats subscribed to t with rebalance timeout 60000 ms and these identity fields
      | member | instance | epoch | owned |
      | m1 | null | 3 | [0] |
    Then the complete consumer response and remaining member epochs and instance owner are
      """
      {
        "response": {
          "error_code": 110,
          "error_message": "The consumer group member has a smaller member epoch (3) than the one known by the group coordinator (5). The member must abandon all its partitions and rejoin.",
          "member_id": null,
          "member_epoch": 0,
          "heartbeat_interval_ms": 0,
          "assignment": null
        },
        "members": [
          [
            "m1",
            5
          ],
          [
            "s1",
            5
          ]
        ],
        "instance_i1": "s1"
      }
      """

  @consumer-identity
  Scenario: An epoch above the coordinator epoch is fenced
    Given consumer group g has group and target epochs 5 with four partitions of topic t UUID 09090909-0909-0909-0909-090909090909 and these settled members
      | member | instance | epoch | previous_epoch | partitions |
      | m1 | null | 5 | 4 | [0,1] |
      | s1 | "i1" | 5 | 4 | [2,3] |
    When that consumer heartbeats subscribed to t with rebalance timeout 60000 ms and these identity fields
      | member | instance | epoch | owned |
      | m1 | null | 6 | [0, 1] |
    Then the complete consumer response and remaining member epochs and instance owner are
      """
      {
        "response": {
          "error_code": 110,
          "error_message": "The consumer group member has a greater member epoch (6) than the one known by the group coordinator (5). The member must abandon all its partitions and rejoin.",
          "member_id": null,
          "member_epoch": 0,
          "heartbeat_interval_ms": 0,
          "assignment": null
        },
        "members": [
          [
            "m1",
            5
          ],
          [
            "s1",
            5
          ]
        ],
        "instance_i1": "s1"
      }
      """

  @consumer-identity
  Scenario: An unknown member cannot leave
    Given consumer group g has group and target epochs 5 with four partitions of topic t UUID 09090909-0909-0909-0909-090909090909 and these settled members
      | member | instance | epoch | previous_epoch | partitions |
      | m1 | null | 5 | 4 | [0,1] |
      | s1 | "i1" | 5 | 4 | [2,3] |
    When that consumer heartbeats subscribed to t with rebalance timeout 60000 ms and these identity fields
      | member | instance | epoch | owned |
      | ghost | null | -1 | null |
    Then the complete consumer response and remaining member epochs and instance owner are
      """
      {
        "response": {
          "error_code": 25,
          "error_message": "Member ghost is not a member of group g.",
          "member_id": null,
          "member_epoch": 0,
          "heartbeat_interval_ms": 0,
          "assignment": null
        },
        "members": [
          [
            "m1",
            5
          ],
          [
            "s1",
            5
          ]
        ],
        "instance_i1": "s1"
      }
      """

  @consumer-identity
  Scenario: A dynamic member leaves with epoch -1
    Given consumer group g has group and target epochs 5 with four partitions of topic t UUID 09090909-0909-0909-0909-090909090909 and these settled members
      | member | instance | epoch | previous_epoch | partitions |
      | m1 | null | 5 | 4 | [0,1] |
      | s1 | "i1" | 5 | 4 | [2,3] |
    When that consumer heartbeats subscribed to t with rebalance timeout 60000 ms and these identity fields
      | member | instance | epoch | owned |
      | m1 | null | -1 | null |
    Then the complete consumer response and remaining member epochs and instance owner are
      """
      {
        "response": {
          "error_code": 0,
          "error_message": null,
          "member_id": "m1",
          "member_epoch": -1,
          "heartbeat_interval_ms": 0,
          "assignment": null
        },
        "members": [
          [
            "s1",
            5
          ]
        ],
        "instance_i1": "s1"
      }
      """

  @consumer-identity
  Scenario: A different member id cannot heartbeat for a static instance
    Given consumer group g has group and target epochs 5 with four partitions of topic t UUID 09090909-0909-0909-0909-090909090909 and these settled members
      | member | instance | epoch | previous_epoch | partitions |
      | m1 | null | 5 | 4 | [0,1] |
      | s1 | "i1" | 5 | 4 | [2,3] |
    When that consumer heartbeats subscribed to t with rebalance timeout 60000 ms and these identity fields
      | member | instance | epoch | owned |
      | m9 | "i1" | 5 | [2, 3] |
    Then the complete consumer response and remaining member epochs and instance owner are
      """
      {
        "response": {
          "error_code": 82,
          "error_message": "Static member m9 with instance id i1 was fenced by member s1.",
          "member_id": null,
          "member_epoch": 0,
          "heartbeat_interval_ms": 0,
          "assignment": null
        },
        "members": [
          [
            "m1",
            5
          ],
          [
            "s1",
            5
          ]
        ],
        "instance_i1": "s1"
      }
      """

  @consumer-identity
  Scenario: A different member id cannot release a static instance
    Given consumer group g has group and target epochs 5 with four partitions of topic t UUID 09090909-0909-0909-0909-090909090909 and these settled members
      | member | instance | epoch | previous_epoch | partitions |
      | m1 | null | 5 | 4 | [0,1] |
      | s1 | "i1" | 5 | 4 | [2,3] |
    When that consumer heartbeats subscribed to t with rebalance timeout 60000 ms and these identity fields
      | member | instance | epoch | owned |
      | m9 | "i1" | -2 | null |
    Then the complete consumer response and remaining member epochs and instance owner are
      """
      {
        "response": {
          "error_code": 82,
          "error_message": "Static member m9 with instance id i1 was fenced by member s1.",
          "member_id": null,
          "member_epoch": 0,
          "heartbeat_interval_ms": 0,
          "assignment": null
        },
        "members": [
          [
            "m1",
            5
          ],
          [
            "s1",
            5
          ]
        ],
        "instance_i1": "s1"
      }
      """

  @consumer-identity
  Scenario: A static heartbeat cannot use an unknown instance
    Given consumer group g has group and target epochs 5 with four partitions of topic t UUID 09090909-0909-0909-0909-090909090909 and these settled members
      | member | instance | epoch | previous_epoch | partitions |
      | m1 | null | 5 | 4 | [0,1] |
      | s1 | "i1" | 5 | 4 | [2,3] |
    When that consumer heartbeats subscribed to t with rebalance timeout 60000 ms and these identity fields
      | member | instance | epoch | owned |
      | s1 | "i9" | 5 | [2, 3] |
    Then the complete consumer response and remaining member epochs and instance owner are
      """
      {
        "response": {
          "error_code": 25,
          "error_message": "Instance id i9 is unknown.",
          "member_id": null,
          "member_epoch": 0,
          "heartbeat_interval_ms": 0,
          "assignment": null
        },
        "members": [
          [
            "m1",
            5
          ],
          [
            "s1",
            5
          ]
        ],
        "instance_i1": "s1"
      }
      """

  Scenario: An offset-only classic group upgrades even with migration disabled
    Given classic group g at generation 1 has 0 members subscribed to one-partition topic t with protocol absent and migration policy disabled
    When native consumer native heartbeats at epoch 0 subscribed to t with rebalance timeout 60000 ms and no owned partitions
    Then the consumer admission error and classic metadata tombstone are
      """
      {"error_code": 0, "error_message": null, "classic_tombstone": true}
      """

  Scenario: An empty connect group is replaced by a consumer group
    Given classic group g at generation 1 has 0 members subscribed to one-partition topic t with protocol connect and migration policy bidirectional
    When native consumer native heartbeats at epoch 0 subscribed to t with rebalance timeout 60000 ms and no owned partitions
    Then the consumer admission error and classic metadata tombstone are
      """
      {"error_code": 0, "error_message": null, "classic_tombstone": true}
      """

  Scenario: A live classic group refuses a consumer join when online upgrade is disabled
    Given classic group g at generation 1 has 1 members subscribed to one-partition topic t with protocol consumer and migration policy disabled
    When native consumer native heartbeats at epoch 0 subscribed to t with rebalance timeout 60000 ms and no owned partitions
    Then the consumer admission error and classic metadata tombstone are
      """
      {"error_code": 69, "error_message": "Cannot upgrade classic group g to consumer group because online upgrade is disabled.", "classic_tombstone": false}
      """

  Scenario: A live classic group upgrades when online upgrade is allowed
    Given classic group g at generation 1 has 1 members subscribed to one-partition topic t with protocol consumer and migration policy upgrade
    When native consumer native heartbeats at epoch 0 subscribed to t with rebalance timeout 60000 ms and no owned partitions
    Then the consumer admission error and classic metadata tombstone are
      """
      {"error_code": 0, "error_message": null, "classic_tombstone": true}
      """

  Scenario: A steady heartbeat cannot create a consumer group over an offset-only group
    Given classic group g at generation 1 has 0 members subscribed to one-partition topic t with protocol absent and migration policy bidirectional
    When native consumer native heartbeats at epoch 3 subscribed to t with rebalance timeout 60000 ms and no owned partitions
    Then the consumer admission error and classic metadata tombstone are
      """
      {"error_code": 69, "error_message": "Group g is not a consumer group.", "classic_tombstone": false}
      """
