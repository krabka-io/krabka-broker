Feature: Kafka transaction isolation over committed and aborted data
  Scenario: commit then read committed sees records
    Given transactional producer my-tid uses an empty topic t
    When the producer commits records a,b,c
    And read_committed group g1 polls until at least 3 records arrive
    Then read_committed sees exactly a,b,c

  Scenario: abort then read committed skips records
    Given transaction abort-tid uses topic ta with 128-byte segments
    When the producer acknowledges and aborts records x,y,z
    And a nontransactional producer appends after and seals the abort marker
    And a read_committed wire fetch starts at offset zero of the sealed transaction log
    Then the sealed segment reports exactly one aborted transaction beginning at offset zero
    When read_committed group g-abort polls until at least 1 records arrive
    Then read_committed sees exactly after
    When read_uncommitted group g-abort-uc polls through the four data records
    Then read_uncommitted sees exactly x,y,z,after

  Scenario: interleaved commit and abort
    Given transactional producer interleave-tid uses an empty topic ti
    When the producer commits records a,b,c
    And the producer acknowledges and aborts records X,Y
    And the producer commits records d,e,f,g
    And read_committed group g-interleave polls until at least 7 records arrive
    Then read_committed sees exactly a,b,c,d,e,f,g
