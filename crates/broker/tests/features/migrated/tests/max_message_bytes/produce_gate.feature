Feature: Record batches obey the configured limits at their stored size
  Scenario: An exact message cap accepts its boundary and refuses one byte more
    Given a broker hosts topics with these message and segment limits
      | topic | message | segment | codec |
      | orders | 2048 | default | producer |
    When a producer appends a 2048 byte wire batch to orders
    Then the batch is accepted at offset 0
    When a producer appends a 2049 byte wire batch to orders
    Then the batch is refused as a message too large
    And topic orders has log end offset 1
    When a producer appends a 2048 byte wire batch to orders
    Then the batch is accepted at offset 1
    And topic orders has log end offset 2

  Scenario: Segment size independently limits a whole record batch
    Given a broker hosts topics with these message and segment limits
      | topic | message | segment | codec |
      | orders | 2097152 | 1048576 | producer |
    When a producer appends a 1048576 byte wire batch to orders
    Then the batch is accepted at offset 0
    When a producer appends a 1048577 byte wire batch to orders
    Then the batch is refused as a record list too large for the segment
    And topic orders has log end offset 1
    When a producer appends a 1048576 byte wire batch to orders
    Then the batch is accepted at offset 1
    And topic orders has log end offset 2

  Scenario: An unconfigured topic inherits the broker message cap
    Given a broker hosts topics with these message and segment limits
      | topic | message | segment | codec |
      | orders | default | default | producer |
    When a producer appends a 1048588 byte wire batch to orders
    Then the batch is accepted at offset 0
    When a producer appends a 1048589 byte wire batch to orders
    Then the batch is refused as a message too large
    And topic orders has log end offset 1

  Scenario: Recompression measures the exact stored LZ4 batch size
    Given a gzip batch of twenty thousand bytes is smaller than its level nine LZ4 storage form
    When a producer appends the gzip batch to under
    Then the batch is refused as a message too large
    And topic under has log end offset 0
    When a producer appends the gzip batch to at
    Then the batch is accepted at offset 0
    And topic at has log end offset 1

  Scenario: Topic overrides tighten and loosen the broker default
    Given a broker hosts topics with these message and segment limits
      | topic | message | segment | codec |
      | tight | 2048 | default | producer |
      | loose | 2097176 | default | producer |
    When a producer appends a 2049 byte wire batch to tight
    Then the batch is refused as a message too large
    And topic tight has log end offset 0
    When a producer appends a 1048589 byte wire batch to loose
    Then the batch is accepted at offset 0

  Scenario: An uncompressed storage policy measures the expanded batch
    Given a broker hosts topics with these message and segment limits
      | topic | message | segment | codec |
      | orders | 2048 | default | uncompressed |
    When a producer appends gzip compressed content of 100000 bytes to orders
    Then the batch is refused as a message too large
    And topic orders has log end offset 0
    When a producer appends gzip compressed content of 100 bytes to orders
    Then the batch is accepted at offset 0
    And topic orders has log end offset 1
