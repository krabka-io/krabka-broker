Feature: Interpreting and generating Kafka replica-management tool documents
  Scenario: Election output distinguishes success no-op and failure per partition
    Given the leader election tool printed
      """
      Successfully completed leader election (PREFERRED) for partitions orders-0
      Valid replica already elected for partitions orders-1, orders-eu-2
      Error completing leader election (PREFERRED) for partition: orders-3: org.apache.kafka.common.errors.PreferredLeaderNotAvailableException: The preferred leader was not available.
      """
    When partition election outcomes are read
    Then the partition election outcomes are exactly
      | topic     | partition | outcome                                                                          |
      | orders    | 0         | elected                                                                          |
      | orders    | 1         | already elected                                                                  |
      | orders-eu | 2         | already elected                                                                  |
      | orders    | 3         | org.apache.kafka.common.errors.PreferredLeaderNotAvailableException                |

  Scenario Outline: Partition labels split at the final hyphen and reject invalid indexes
    Given a tool partition label <text>
    When the topic name and partition index are read from the label
    Then the topic-partition label resolves to <result>

    Examples:
      | text              | result              |
      | orders-eu-west-12 | orders-eu-west / 12 |
      | orders            | invalid             |
      | orders-x          | invalid             |

  Scenario: A plural cancellation sentence reports both cancelled partitions
    Given the reassignment cancellation tool printed
      """
      Successfully cancelled partition reassignments for: orders-0,orders-1
      """
    When cancelled partitions are read
    Then the cancelled partitions are orders:0,orders:1

  Scenario: An absent reassignment plan reports no cancelled partitions
    Given the reassignment cancellation tool printed
      """
      None of the specified partition reassignments exist.
      """
    When cancelled partitions are read
    Then the cancelled partitions are none

  Scenario: A singular cancellation sentence still identifies its partition
    Given the reassignment cancellation tool printed
      """
      Successfully cancelled partition reassignment for: orders-0
      None of the specified partition moves are active.
      """
    When cancelled partitions are read
    Then the cancelled partitions are orders:0

  Scenario: Generated plans retain separate current and proposed replica assignments
    Given the reassignment generation tool printed
      """
      Current partition replica assignment
      {"version":1,"partitions":[{"topic":"orders","partition":0,"replicas":[1,2],"log_dirs":["any","any"]}]}

      Proposed partition reassignment configuration
      {"version":1,"partitions":[{"topic":"orders","partition":0,"replicas":[2,3],"log_dirs":["any","any"]}]}
      """
    When the current and proposed assignments are read
    Then the current assignment is exactly
      | topic  | partition | replicas |
      | orders | 0         | 1,2      |
    And the proposed assignment is exactly
      | topic  | partition | replicas |
      | orders | 0         | 2,3      |

  Scenario: Prose without labelled plans does not invent an assignment
    Given the reassignment generation tool printed
      """
      nothing was generated
      """
    When the current and proposed assignments are read
    Then no generated assignment is reported

  Scenario: Assignment order in JSON does not affect normalized partition order
    Given the same assignment document is printed in opposite partition orders
      """
      {"version":1,"partitions":[{"topic":"t","partition":1,"replicas":[2]},{"topic":"t","partition":0,"replicas":[1]}]}
      """
    When each assignment document is read
    Then every document yields the same sorted assignments
      | topic | partition | replicas |
      | t     | 0         | 1        |
      | t     | 1         | 2        |

  Scenario: Generated tool input documents preserve their requested assignments and wire shape
    Given the requested partition assignments are
      | topic  | partition | replicas |
      | orders | 0         | 1,2      |
      | orders | 1         | 2,3      |
    When reassignment election and topic-move input documents are generated
    Then the reassignment document preserves every requested replica
    And the election input JSON is
      """
      {"partitions":[{"partition":0,"topic":"orders"}]}
      """
    And the topic-move input JSON is
      """
      {"topics":[{"topic":"orders"}],"version":1}
      """
