Feature: Hosted classic members use explicit consumer state and classic RPC fields
  Scenario: Hosted SyncGroup 1 a member at the target epoch
    Given consumer group g at target epoch 5 has topic t with two partitions and these complete members
      """
      [
        {
          "assigned": {
            "7": [
              0
            ]
          },
          "assignment_epochs": {},
          "assignor": null,
          "classic": {
            "protocols": [
              {
                "metadata": "00010000",
                "name": "range"
              },
              {
                "metadata": "00010000",
                "name": "roundrobin"
              }
            ],
            "session_ms": 45000
          },
          "client": "c",
          "epoch": 5,
          "host": "/127.0.0.1",
          "instance": null,
          "member": "m",
          "previous_epoch": 4,
          "rack": null,
          "rebalance_ms": 60000,
          "regex": null,
          "revoking": {},
          "state": "Stable",
          "subscriptions": [
            "t"
          ]
        }
      ]
      """
    And the hosted classic SyncGroup request has these fields
      """
      {
        "generation": 5,
        "instance": null,
        "member": "m",
        "protocol_name": "range",
        "protocol_type": "consumer"
      }
      """
    When the consumer coordinator serves the hosted classic SyncGroup request
    Then the complete hosted classic SyncGroup response is
      """
      {
        "assignment": "0001000000010001740000000100000000ffffffff",
        "error": 0,
        "protocol_name": "range",
        "protocol_type": "consumer"
      }
      """
  Scenario: Hosted SyncGroup 2 a request without protocol type and name
    Given consumer group g at target epoch 5 has topic t with two partitions and these complete members
      """
      [
        {
          "assigned": {
            "7": [
              0
            ]
          },
          "assignment_epochs": {},
          "assignor": null,
          "classic": {
            "protocols": [
              {
                "metadata": "00010000",
                "name": "range"
              },
              {
                "metadata": "00010000",
                "name": "roundrobin"
              }
            ],
            "session_ms": 45000
          },
          "client": "c",
          "epoch": 5,
          "host": "/127.0.0.1",
          "instance": null,
          "member": "m",
          "previous_epoch": 4,
          "rack": null,
          "rebalance_ms": 60000,
          "regex": null,
          "revoking": {},
          "state": "Stable",
          "subscriptions": [
            "t"
          ]
        }
      ]
      """
    And the hosted classic SyncGroup request has these fields
      """
      {
        "generation": 5,
        "instance": null,
        "member": "m",
        "protocol_name": null,
        "protocol_type": null
      }
      """
    When the consumer coordinator serves the hosted classic SyncGroup request
    Then the complete hosted classic SyncGroup response is
      """
      {
        "assignment": "0001000000010001740000000100000000ffffffff",
        "error": 0,
        "protocol_name": null,
        "protocol_type": null
      }
      """
  Scenario: Hosted SyncGroup 3 a member revoking partitions after the target moved
    Given consumer group g at target epoch 5 has topic t with two partitions and these complete members
      """
      [
        {
          "assigned": {
            "7": [
              0
            ]
          },
          "assignment_epochs": {},
          "assignor": null,
          "classic": {
            "protocols": [
              {
                "metadata": "00010000",
                "name": "range"
              },
              {
                "metadata": "00010000",
                "name": "roundrobin"
              }
            ],
            "session_ms": 45000
          },
          "client": "c",
          "epoch": 4,
          "host": "/127.0.0.1",
          "instance": null,
          "member": "m",
          "previous_epoch": 4,
          "rack": null,
          "rebalance_ms": 60000,
          "regex": null,
          "revoking": {},
          "state": "UnrevokedPartitions",
          "subscriptions": [
            "t"
          ]
        }
      ]
      """
    And the hosted classic SyncGroup request has these fields
      """
      {
        "generation": 4,
        "instance": null,
        "member": "m",
        "protocol_name": "range",
        "protocol_type": "consumer"
      }
      """
    When the consumer coordinator serves the hosted classic SyncGroup request
    Then the complete hosted classic SyncGroup response is
      """
      {
        "assignment": "0001000000010001740000000100000000ffffffff",
        "error": 0,
        "protocol_name": "range",
        "protocol_type": "consumer"
      }
      """
  Scenario: Hosted SyncGroup 4 an absent member
    Given consumer group g at target epoch 5 has topic t with two partitions and these complete members
      """
      [
        {
          "assigned": {
            "7": [
              0
            ]
          },
          "assignment_epochs": {},
          "assignor": null,
          "classic": {
            "protocols": [
              {
                "metadata": "00010000",
                "name": "range"
              },
              {
                "metadata": "00010000",
                "name": "roundrobin"
              }
            ],
            "session_ms": 45000
          },
          "client": "c",
          "epoch": 5,
          "host": "/127.0.0.1",
          "instance": null,
          "member": "m",
          "previous_epoch": 4,
          "rack": null,
          "rebalance_ms": 60000,
          "regex": null,
          "revoking": {},
          "state": "Stable",
          "subscriptions": [
            "t"
          ]
        }
      ]
      """
    And the hosted classic SyncGroup request has these fields
      """
      {
        "generation": 5,
        "instance": null,
        "member": "nobody",
        "protocol_name": "range",
        "protocol_type": "consumer"
      }
      """
    When the consumer coordinator serves the hosted classic SyncGroup request
    Then the complete hosted classic SyncGroup response is
      """
      {
        "assignment": "",
        "error": 25,
        "protocol_name": null,
        "protocol_type": null
      }
      """
  Scenario: Hosted SyncGroup 5 a member of the consumer protocol
    Given consumer group g at target epoch 5 has topic t with two partitions and these complete members
      """
      [
        {
          "assigned": {
            "7": [
              0
            ]
          },
          "assignment_epochs": {},
          "assignor": null,
          "classic": null,
          "client": "c",
          "epoch": 5,
          "host": "/127.0.0.1",
          "instance": null,
          "member": "m",
          "previous_epoch": 4,
          "rack": null,
          "rebalance_ms": 60000,
          "regex": null,
          "revoking": {},
          "state": "Stable",
          "subscriptions": [
            "t"
          ]
        }
      ]
      """
    And the hosted classic SyncGroup request has these fields
      """
      {
        "generation": 5,
        "instance": null,
        "member": "m",
        "protocol_name": "range",
        "protocol_type": "consumer"
      }
      """
    When the consumer coordinator serves the hosted classic SyncGroup request
    Then the complete hosted classic SyncGroup response is
      """
      {
        "assignment": "",
        "error": 25,
        "protocol_name": null,
        "protocol_type": null
      }
      """
  Scenario: Hosted SyncGroup 6 an unknown instance id
    Given consumer group g at target epoch 5 has topic t with two partitions and these complete members
      """
      [
        {
          "assigned": {
            "7": [
              0
            ]
          },
          "assignment_epochs": {},
          "assignor": null,
          "classic": {
            "protocols": [
              {
                "metadata": "00010000",
                "name": "range"
              },
              {
                "metadata": "00010000",
                "name": "roundrobin"
              }
            ],
            "session_ms": 45000
          },
          "client": "c",
          "epoch": 5,
          "host": "/127.0.0.1",
          "instance": null,
          "member": "m",
          "previous_epoch": 4,
          "rack": null,
          "rebalance_ms": 60000,
          "regex": null,
          "revoking": {},
          "state": "Stable",
          "subscriptions": [
            "t"
          ]
        }
      ]
      """
    And the hosted classic SyncGroup request has these fields
      """
      {
        "generation": 5,
        "instance": "instance-1",
        "member": "m",
        "protocol_name": "range",
        "protocol_type": "consumer"
      }
      """
    When the consumer coordinator serves the hosted classic SyncGroup request
    Then the complete hosted classic SyncGroup response is
      """
      {
        "assignment": "",
        "error": 25,
        "protocol_name": null,
        "protocol_type": null
      }
      """
  Scenario: Hosted SyncGroup 7 an instance id another member holds
    Given consumer group g at target epoch 5 has topic t with two partitions and these complete members
      """
      [
        {
          "assigned": {
            "7": [
              0
            ]
          },
          "assignment_epochs": {},
          "assignor": null,
          "classic": {
            "protocols": [
              {
                "metadata": "00010000",
                "name": "range"
              },
              {
                "metadata": "00010000",
                "name": "roundrobin"
              }
            ],
            "session_ms": 45000
          },
          "client": "c",
          "epoch": 5,
          "host": "/127.0.0.1",
          "instance": "instance-1",
          "member": "m",
          "previous_epoch": 4,
          "rack": null,
          "rebalance_ms": 60000,
          "regex": null,
          "revoking": {},
          "state": "Stable",
          "subscriptions": [
            "t"
          ]
        }
      ]
      """
    And the hosted classic SyncGroup request has these fields
      """
      {
        "generation": 5,
        "instance": "instance-1",
        "member": "other",
        "protocol_name": "range",
        "protocol_type": "consumer"
      }
      """
    When the consumer coordinator serves the hosted classic SyncGroup request
    Then the complete hosted classic SyncGroup response is
      """
      {
        "assignment": "",
        "error": 82,
        "protocol_name": null,
        "protocol_type": null
      }
      """
  Scenario: Hosted SyncGroup 8 a generation that is not the member epoch
    Given consumer group g at target epoch 5 has topic t with two partitions and these complete members
      """
      [
        {
          "assigned": {
            "7": [
              0
            ]
          },
          "assignment_epochs": {},
          "assignor": null,
          "classic": {
            "protocols": [
              {
                "metadata": "00010000",
                "name": "range"
              },
              {
                "metadata": "00010000",
                "name": "roundrobin"
              }
            ],
            "session_ms": 45000
          },
          "client": "c",
          "epoch": 5,
          "host": "/127.0.0.1",
          "instance": null,
          "member": "m",
          "previous_epoch": 4,
          "rack": null,
          "rebalance_ms": 60000,
          "regex": null,
          "revoking": {},
          "state": "Stable",
          "subscriptions": [
            "t"
          ]
        }
      ]
      """
    And the hosted classic SyncGroup request has these fields
      """
      {
        "generation": 6,
        "instance": null,
        "member": "m",
        "protocol_name": "range",
        "protocol_type": "consumer"
      }
      """
    When the consumer coordinator serves the hosted classic SyncGroup request
    Then the complete hosted classic SyncGroup response is
      """
      {
        "assignment": "",
        "error": 22,
        "protocol_name": null,
        "protocol_type": null
      }
      """
  Scenario: Hosted SyncGroup 9 another protocol type
    Given consumer group g at target epoch 5 has topic t with two partitions and these complete members
      """
      [
        {
          "assigned": {
            "7": [
              0
            ]
          },
          "assignment_epochs": {},
          "assignor": null,
          "classic": {
            "protocols": [
              {
                "metadata": "00010000",
                "name": "range"
              },
              {
                "metadata": "00010000",
                "name": "roundrobin"
              }
            ],
            "session_ms": 45000
          },
          "client": "c",
          "epoch": 5,
          "host": "/127.0.0.1",
          "instance": null,
          "member": "m",
          "previous_epoch": 4,
          "rack": null,
          "rebalance_ms": 60000,
          "regex": null,
          "revoking": {},
          "state": "Stable",
          "subscriptions": [
            "t"
          ]
        }
      ]
      """
    And the hosted classic SyncGroup request has these fields
      """
      {
        "generation": 5,
        "instance": null,
        "member": "m",
        "protocol_name": "range",
        "protocol_type": "connect"
      }
      """
    When the consumer coordinator serves the hosted classic SyncGroup request
    Then the complete hosted classic SyncGroup response is
      """
      {
        "assignment": "",
        "error": 23,
        "protocol_name": null,
        "protocol_type": null
      }
      """
  Scenario: Hosted SyncGroup 10 a protocol other than the member's first
    Given consumer group g at target epoch 5 has topic t with two partitions and these complete members
      """
      [
        {
          "assigned": {
            "7": [
              0
            ]
          },
          "assignment_epochs": {},
          "assignor": null,
          "classic": {
            "protocols": [
              {
                "metadata": "00010000",
                "name": "range"
              },
              {
                "metadata": "00010000",
                "name": "roundrobin"
              }
            ],
            "session_ms": 45000
          },
          "client": "c",
          "epoch": 5,
          "host": "/127.0.0.1",
          "instance": null,
          "member": "m",
          "previous_epoch": 4,
          "rack": null,
          "rebalance_ms": 60000,
          "regex": null,
          "revoking": {},
          "state": "Stable",
          "subscriptions": [
            "t"
          ]
        }
      ]
      """
    And the hosted classic SyncGroup request has these fields
      """
      {
        "generation": 5,
        "instance": null,
        "member": "m",
        "protocol_name": "roundrobin",
        "protocol_type": "consumer"
      }
      """
    When the consumer coordinator serves the hosted classic SyncGroup request
    Then the complete hosted classic SyncGroup response is
      """
      {
        "assignment": "",
        "error": 23,
        "protocol_name": null,
        "protocol_type": null
      }
      """
  Scenario: Hosted SyncGroup 11 a stable member that the target moved past
    Given consumer group g at target epoch 5 has topic t with two partitions and these complete members
      """
      [
        {
          "assigned": {
            "7": [
              0
            ]
          },
          "assignment_epochs": {},
          "assignor": null,
          "classic": {
            "protocols": [
              {
                "metadata": "00010000",
                "name": "range"
              },
              {
                "metadata": "00010000",
                "name": "roundrobin"
              }
            ],
            "session_ms": 45000
          },
          "client": "c",
          "epoch": 4,
          "host": "/127.0.0.1",
          "instance": null,
          "member": "m",
          "previous_epoch": 4,
          "rack": null,
          "rebalance_ms": 60000,
          "regex": null,
          "revoking": {},
          "state": "Stable",
          "subscriptions": [
            "t"
          ]
        }
      ]
      """
    And the hosted classic SyncGroup request has these fields
      """
      {
        "generation": 4,
        "instance": null,
        "member": "m",
        "protocol_name": "range",
        "protocol_type": "consumer"
      }
      """
    When the consumer coordinator serves the hosted classic SyncGroup request
    Then the complete hosted classic SyncGroup response is
      """
      {
        "assignment": "",
        "error": 27,
        "protocol_name": null,
        "protocol_type": null
      }
      """
  Scenario Outline: Classic heartbeats request rejoin when reconciliation can advance
    Given hosted classic member m is at epoch <epoch> assignment state <assignment> with its waiting partition held elsewhere <held>
    When classic member m instance absent heartbeats at generation member
    Then the hosted classic heartbeat returns Kafka error code <code>
    Examples:
      | epoch | assignment | held | code |
      | 5 | stable | false | 0 |
      | 4 | stable | false | 27 |
      | 5 | unrevoked | false | 27 |
      | 5 | unreleased | true | 0 |
      | 5 | unreleased | false | 27 |
  Scenario Outline: Classic hosted heartbeat validates member instance and generation identity
    Given consumer group g hosts classic member m at static instance instance-1 and native member native
    When classic member <member> instance <instance> heartbeats at generation <generation>
    Then the hosted classic heartbeat returns Kafka error code <code>
    Examples:
      | member | instance | generation | code |
      | nobody | absent | target | 25 |
      | native | absent | target | 25 |
      | m | instance-2 | target | 25 |
      | other | instance-1 | target | 82 |
      | m | instance-1 | zero | 22 |
