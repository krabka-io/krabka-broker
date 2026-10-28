Feature: Streams internal topic configuration exposes full topology inputs and outcomes
  Scenario: 1 copartition coerces the flexible repartition topic
    Given the Streams application publishes this complete topology
      """
      {
        "epoch": 1,
        "subtopologies": [
          {
            "copartition_groups": [],
            "repartition_sink_topics": [
              "rp"
            ],
            "repartition_source_topics": [],
            "source_topic_regex": [],
            "source_topics": [
              "orders"
            ],
            "state_changelog_topics": [],
            "subtopology_id": "0"
          },
          {
            "copartition_groups": [
              {
                "repartition_source_topics": [
                  0
                ],
                "source_topic_regex": [],
                "source_topics": [
                  0
                ]
              }
            ],
            "repartition_sink_topics": [],
            "repartition_source_topics": [
              {
                "name": "rp",
                "partitions": 0,
                "replication_factor": 0,
                "topic_configs": []
              }
            ],
            "source_topic_regex": [],
            "source_topics": [
              "customers"
            ],
            "state_changelog_topics": [
              {
                "name": "store-changelog",
                "partitions": 0,
                "replication_factor": 0,
                "topic_configs": []
              }
            ],
            "subtopology_id": "1"
          }
        ]
      }
      """
    And the metadata image contains these topic partition counts
      | name | id | partitions |
      | customers | 2 | 3 |
      | orders | 1 | 6 |
    When the internal topic manager configures the topology against the metadata image
    Then the complete configured topology or exact configuration error is
      """
      {
        "internal_topics_to_create": {
          "rp": {
            "configs": {},
            "name": "rp",
            "partitions": 3,
            "replication_factor": null
          },
          "store-changelog": {
            "configs": {},
            "name": "store-changelog",
            "partitions": 3,
            "replication_factor": null
          }
        },
        "status": {
          "code": 3,
          "detail": "Internal topics are missing: rp, store-changelog"
        },
        "subtopologies": {
          "0": {
            "number_of_tasks": 6,
            "repartition_sink_topics": [
              "rp"
            ],
            "repartition_source_topics": {},
            "source_topics": [
              "orders"
            ],
            "state_changelog_topics": {}
          },
          "1": {
            "number_of_tasks": 3,
            "repartition_sink_topics": [],
            "repartition_source_topics": {
              "rp": {
                "configs": {},
                "name": "rp",
                "partitions": 3,
                "replication_factor": null
              }
            },
            "source_topics": [
              "customers"
            ],
            "state_changelog_topics": {
              "store-changelog": {
                "configs": {},
                "name": "store-changelog",
                "partitions": 3,
                "replication_factor": null
              }
            }
          }
        },
        "topology_epoch": 1
      }
      """
  Scenario: 2 a flexible repartition topic takes its writer's partition count
    Given the Streams application publishes this complete topology
      """
      {
        "epoch": 1,
        "subtopologies": [
          {
            "copartition_groups": [],
            "repartition_sink_topics": [
              "rp"
            ],
            "repartition_source_topics": [],
            "source_topic_regex": [],
            "source_topics": [
              "in"
            ],
            "state_changelog_topics": [],
            "subtopology_id": "0"
          },
          {
            "copartition_groups": [],
            "repartition_sink_topics": [],
            "repartition_source_topics": [
              {
                "name": "rp",
                "partitions": 0,
                "replication_factor": 0,
                "topic_configs": []
              }
            ],
            "source_topic_regex": [],
            "source_topics": [
              "other"
            ],
            "state_changelog_topics": [],
            "subtopology_id": "1"
          }
        ]
      }
      """
    And the metadata image contains these topic partition counts
      | name | id | partitions |
      | in | 1 | 4 |
      | other | 2 | 8 |
      | rp | 3 | 4 |
    When the internal topic manager configures the topology against the metadata image
    Then the complete configured topology or exact configuration error is
      """
      {
        "internal_topics_to_create": {},
        "status": null,
        "subtopologies": {
          "0": {
            "number_of_tasks": 4,
            "repartition_sink_topics": [
              "rp"
            ],
            "repartition_source_topics": {},
            "source_topics": [
              "in"
            ],
            "state_changelog_topics": {}
          },
          "1": {
            "number_of_tasks": 8,
            "repartition_sink_topics": [],
            "repartition_source_topics": {
              "rp": {
                "configs": {},
                "name": "rp",
                "partitions": 4,
                "replication_factor": null
              }
            },
            "source_topics": [
              "other"
            ],
            "state_changelog_topics": {}
          }
        },
        "topology_epoch": 1
      }
      """
  Scenario: 3 fixed repartition topics disagree
    Given the Streams application publishes this complete topology
      """
      {
        "epoch": 1,
        "subtopologies": [
          {
            "copartition_groups": [],
            "repartition_sink_topics": [
              "rp1",
              "rp2"
            ],
            "repartition_source_topics": [],
            "source_topic_regex": [],
            "source_topics": [
              "in"
            ],
            "state_changelog_topics": [],
            "subtopology_id": "0"
          },
          {
            "copartition_groups": [
              {
                "repartition_source_topics": [
                  0,
                  1
                ],
                "source_topic_regex": [],
                "source_topics": []
              }
            ],
            "repartition_sink_topics": [],
            "repartition_source_topics": [
              {
                "name": "rp1",
                "partitions": 2,
                "replication_factor": 0,
                "topic_configs": []
              },
              {
                "name": "rp2",
                "partitions": 3,
                "replication_factor": 0,
                "topic_configs": []
              }
            ],
            "source_topic_regex": [],
            "source_topics": [],
            "state_changelog_topics": [],
            "subtopology_id": "1"
          }
        ]
      }
      """
    And the metadata image contains these topic partition counts
      | name | id | partitions |
      | in | 1 | 2 |
    When the internal topic manager configures the topology against the metadata image
    Then the complete configured topology or exact configuration error is
      """
      {
        "internal_topics_to_create": {},
        "status": {
          "code": 2,
          "detail": "Following topics do not have the same number of partitions: [{rp1=2, rp2=3}]"
        },
        "subtopologies": null,
        "topology_epoch": 1
      }
      """
  Scenario: 4 external copartitioned topics disagree
    Given the Streams application publishes this complete topology
      """
      {
        "epoch": 1,
        "subtopologies": [
          {
            "copartition_groups": [
              {
                "repartition_source_topics": [],
                "source_topic_regex": [],
                "source_topics": [
                  0,
                  1
                ]
              }
            ],
            "repartition_sink_topics": [],
            "repartition_source_topics": [],
            "source_topic_regex": [],
            "source_topics": [
              "a",
              "b"
            ],
            "state_changelog_topics": [],
            "subtopology_id": "0"
          }
        ]
      }
      """
    And the metadata image contains these topic partition counts
      | name | id | partitions |
      | a | 1 | 2 |
      | b | 2 | 3 |
    When the internal topic manager configures the topology against the metadata image
    Then the complete configured topology or exact configuration error is
      """
      {
        "internal_topics_to_create": {},
        "status": {
          "code": 2,
          "detail": "Following topics do not have the same number of partitions: [{a=2, b=3}]"
        },
        "subtopologies": null,
        "topology_epoch": 1
      }
      """
  Scenario: 5 an existing changelog topic has another partition count
    Given the Streams application publishes this complete topology
      """
      {
        "epoch": 1,
        "subtopologies": [
          {
            "copartition_groups": [],
            "repartition_sink_topics": [],
            "repartition_source_topics": [],
            "source_topic_regex": [],
            "source_topics": [
              "in"
            ],
            "state_changelog_topics": [
              {
                "name": "store-changelog",
                "partitions": 0,
                "replication_factor": 0,
                "topic_configs": []
              }
            ],
            "subtopology_id": "0"
          }
        ]
      }
      """
    And the metadata image contains these topic partition counts
      | name | id | partitions |
      | in | 1 | 2 |
      | store-changelog | 2 | 5 |
    When the internal topic manager configures the topology against the metadata image
    Then the complete configured topology or exact configuration error is
      """
      {
        "internal_topics_to_create": {},
        "status": {
          "code": 2,
          "detail": "Existing topic store-changelog has different number of partitions: expected 2, found 5"
        },
        "subtopologies": null,
        "topology_epoch": 1
      }
      """
  Scenario: 6 missing source topics stop before any internal topic
    Given the Streams application publishes this complete topology
      """
      {
        "epoch": 1,
        "subtopologies": [
          {
            "copartition_groups": [],
            "repartition_sink_topics": [],
            "repartition_source_topics": [],
            "source_topic_regex": [],
            "source_topics": [
              "b",
              "a",
              "c",
              "d",
              "e"
            ],
            "state_changelog_topics": [
              {
                "name": "store-changelog",
                "partitions": 0,
                "replication_factor": 0,
                "topic_configs": []
              }
            ],
            "subtopology_id": "0"
          }
        ]
      }
      """
    And the metadata image contains these topic partition counts
      | name | id | partitions |
      | c | 1 | 1 |
    When the internal topic manager configures the topology against the metadata image
    Then the complete configured topology or exact configuration error is
      """
      {
        "internal_topics_to_create": {},
        "status": {
          "code": 1,
          "detail": "Source topics a, b, d and 1 additional topics are missing."
        },
        "subtopologies": null,
        "topology_epoch": 1
      }
      """
  Scenario: 7 a repartition source topic is never written
    Given the Streams application publishes this complete topology
      """
      {
        "epoch": 1,
        "subtopologies": [
          {
            "copartition_groups": [],
            "repartition_sink_topics": [],
            "repartition_source_topics": [
              {
                "name": "rp",
                "partitions": 0,
                "replication_factor": 0,
                "topic_configs": []
              }
            ],
            "source_topic_regex": [],
            "source_topics": [
              "in"
            ],
            "state_changelog_topics": [],
            "subtopology_id": "0"
          }
        ]
      }
      """
    And the metadata image contains these topic partition counts
      | name | id | partitions |
      | in | 1 | 1 |
    When the internal topic manager configures the topology against the metadata image
    Then the complete configured topology or exact configuration error is
      """
      {
        "error_kind": "invalid-topology",
        "message": "Failed to compute number of partitions for all repartition topics, because a repartition source topic is never used as a sink topic."
      }
      """
  Scenario: 8 repartition topics loop
    Given the Streams application publishes this complete topology
      """
      {
        "epoch": 1,
        "subtopologies": [
          {
            "copartition_groups": [],
            "repartition_sink_topics": [
              "rp-a"
            ],
            "repartition_source_topics": [
              {
                "name": "rp-b",
                "partitions": 0,
                "replication_factor": 0,
                "topic_configs": []
              }
            ],
            "source_topic_regex": [],
            "source_topics": [],
            "state_changelog_topics": [],
            "subtopology_id": "0"
          },
          {
            "copartition_groups": [],
            "repartition_sink_topics": [
              "rp-b"
            ],
            "repartition_source_topics": [
              {
                "name": "rp-a",
                "partitions": 0,
                "replication_factor": 0,
                "topic_configs": []
              }
            ],
            "source_topic_regex": [],
            "source_topics": [],
            "state_changelog_topics": [],
            "subtopology_id": "1"
          }
        ]
      }
      """
    And the metadata image contains these topic partition counts
      | name | id | partitions |
    When the internal topic manager configures the topology against the metadata image
    Then the complete configured topology or exact configuration error is
      """
      {
        "error_kind": "invalid-topology",
        "message": "Failed to compute number of partitions for all repartition topics. There may be loops in the topology that cannot be resolved."
      }
      """
  Scenario: 9 a subtopology has no source topics
    Given the Streams application publishes this complete topology
      """
      {
        "epoch": 1,
        "subtopologies": [
          {
            "copartition_groups": [],
            "repartition_sink_topics": [],
            "repartition_source_topics": [],
            "source_topic_regex": [],
            "source_topics": [],
            "state_changelog_topics": [],
            "subtopology_id": "0"
          }
        ]
      }
      """
    And the metadata image contains these topic partition counts
      | name | id | partitions |
    When the internal topic manager configures the topology against the metadata image
    Then the complete configured topology or exact configuration error is
      """
      {
        "error_kind": "invalid-topology",
        "message": "No source topics found for subtopology 0"
      }
      """
  Scenario: 10 an internal topic name is invalid
    Given the Streams application publishes this complete topology
      """
      {
        "epoch": 1,
        "subtopologies": [
          {
            "copartition_groups": [],
            "repartition_sink_topics": [],
            "repartition_source_topics": [],
            "source_topic_regex": [],
            "source_topics": [
              "in"
            ],
            "state_changelog_topics": [
              {
                "name": "bad/name",
                "partitions": 0,
                "replication_factor": 0,
                "topic_configs": []
              }
            ],
            "subtopology_id": "0"
          }
        ]
      }
      """
    And the metadata image contains these topic partition counts
      | name | id | partitions |
      | in | 1 | 1 |
    When the internal topic manager configures the topology against the metadata image
    Then the complete configured topology or exact configuration error is
      """
      {
        "error_kind": "invalid-topic",
        "message": "Topic name is invalid: 'bad/name' contains one or more characters other than ASCII alphanumerics, '.', '_' and '-'"
      }
      """
