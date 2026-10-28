Feature: Topic freezes leave the cluster readable and observable
  Scenario: fetch metadata and the metrics endpoint still answer for a frozen topic
    Given orders contains a frozen record and the broker exposes metrics
    When a consumer fetches orders from offset zero after its freeze
    Then the frozen topic still returns its one existing record without an error
    When a client requests metadata for frozen orders
    Then metadata still exposes the frozen topic and its one partition
    When an observer scrapes the broker metrics after the freeze is published
    Then metrics report one active freeze and one rejected orders write
    When the read-path control topic receives a new record
    Then the read-path control write is accepted at log end one

  Scenario: offset commit still works against a frozen topic
    Given orders contains a frozen record and the group coordinator is ready
    When drainers commit offset one for frozen orders and the unfrozen control
    Then both frozen and unfrozen consumer offset commits succeed
    When the read-path control topic receives a new record
    Then the read-path control write is accepted at log end one
