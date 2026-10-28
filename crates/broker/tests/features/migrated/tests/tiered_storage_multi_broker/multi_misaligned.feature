Feature: A new leader resumes copies from remote offset coverage despite different segment boundaries

  Scenario: a new leader resumes the copy from the tiers coverage
    Given three tiered brokers roll at 1024,1536,1024 bytes and the data partition is assigned to replicas 2,1 without a segment-size override
    When broker 2 appends seventy before records and both replicas' segment boundaries are sampled
    Then the two replicas have different local segment boundaries
    When broker 2 appends seventy before-more records and copies at least two segments
    Then broker 1 evicts a copied segment before becoming leader, proving its metadata has arrived
    When broker 2 shuts down, broker 1 becomes writable leader and appends 140 after records
    Then the survivor copies more segments, reports uncopied lag, evicts sealed local segments and the archive has gapless coverage through its sealed range without redundant objects
