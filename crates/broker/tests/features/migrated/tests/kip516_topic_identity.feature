Feature: Immutable topic identity across Kafka APIs
  A topic id selects a topic independently of its name. Unknown ids are echoed
  in errors for each requested partition and do not invalidate known topics.

  Scenario: An unknown id is echoed in a nameless metadata error
    Given a broker with an unassigned nonzero metadata topic id
    When metadata is requested by the unassigned id alone
    Then metadata returns UNKNOWN_TOPIC_ID with the requested identity and no name

  Scenario: Topic identity takes precedence over a conflicting name
    Given topics m_a and m_b have distinct identities
    When metadata names m_a but supplies the identity of m_b
    Then the metadata describes m_b exactly as a request by its name does

  Scenario: Deleting an unknown topic id preserves its identity in the error
    Given a broker with no topic matching the deletion id
    When that topic id is deleted without supplying a name
    Then deletion refuses the unknown topic id and echoes it

  Scenario: Fetching an unresolved identity refuses every requested partition
    Given a known fetch topic beside zero and unassigned topic ids
    When partitions zero and one are fetched by each unresolved id
    Then both fetched partitions report UNKNOWN_TOPIC_ID under the unchanged id

  Scenario: Producing to an unresolved identity refuses every requested partition
    Given a known produce topic beside zero and unassigned topic ids
    When an empty batch is produced to partitions zero and one of each unresolved id
    Then both produced partitions report UNKNOWN_TOPIC_ID without assigning offsets

  Scenario Outline: An offset committed by identity can be fetched without the topic name
    Given topic <topic> exists for group <group>
    When offset <offset> is committed using the topic id
    And committed offsets are fetched for <selection>
    Then partition zero returns the committed offset with the same topic identity

    Examples:
      | topic    | group | offset | selection        |
      | o_topic  | g1    | 42     | the listed topic |
      | fa_topic | g3    | 7      | all topics       |

  Scenario: Offset fetch echoes unresolved topic identities in its errors
    Given a ready group coordinator and zero and unassigned offset topic ids
    When committed offsets are requested for each unresolved topic id
    Then each unresolved fetch echoes its topic id with UNKNOWN_TOPIC_ID and offset minus one

  Scenario: One unknown topic does not prevent a known topic offset commit
    Given one known topic beside zero and unassigned offset commit topic ids
    When offsets five and nine are committed for the known and each unresolved topic id
    Then each commit refuses the unknown id before acknowledging the known topic
