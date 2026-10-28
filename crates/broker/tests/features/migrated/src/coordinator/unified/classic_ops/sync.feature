Feature: Classic SyncGroup state and identity validation
  Scenario: A preparing leader must rejoin
    Given classic group g is preparing with m1 leader and m2 follower
    And the SyncGroup request proposes new-L and new-F assignments with identity
      | member | instance | generation_delta | protocol_type | protocol_name |
      | m1 | absent | 0 | absent | absent |
    When the coordinator handles the classic SyncGroup request
    Then SyncGroup returns rebalance and leaves group state preparing changing assignments only on leader installation
  Scenario: A preparing follower must rejoin
    Given classic group g is preparing with m1 leader and m2 follower
    And the SyncGroup request proposes new-L and new-F assignments with identity
      | member | instance | generation_delta | protocol_type | protocol_name |
      | m2 | absent | 0 | absent | absent |
    When the coordinator handles the classic SyncGroup request
    Then SyncGroup returns rebalance and leaves group state preparing changing assignments only on leader installation
  Scenario: A completing follower parks until the leader installs
    Given classic group g is completing with m1 leader and m2 follower
    And the SyncGroup request proposes new-L and new-F assignments with identity
      | member | instance | generation_delta | protocol_type | protocol_name |
      | m2 | absent | 0 | absent | absent |
    When the coordinator handles the classic SyncGroup request
    Then SyncGroup returns park and leaves group state completing changing assignments only on leader installation
  Scenario: A completing leader installs the proposed assignments
    Given classic group g is completing with m1 leader and m2 follower
    And the SyncGroup request proposes new-L and new-F assignments with identity
      | member | instance | generation_delta | protocol_type | protocol_name |
      | m1 | absent | 0 | absent | absent |
    When the coordinator handles the classic SyncGroup request
    Then SyncGroup returns install-new-L and leaves group state stable changing assignments only on leader installation
  Scenario: A stable follower reads its current assignment
    Given classic group g is stable with m1 leader and m2 follower
    And the SyncGroup request proposes new-L and new-F assignments with identity
      | member | instance | generation_delta | protocol_type | protocol_name |
      | m2 | absent | 0 | absent | absent |
    When the coordinator handles the classic SyncGroup request
    Then SyncGroup returns read-F and leaves group state stable changing assignments only on leader installation
  Scenario: A stable leader reads its current assignment
    Given classic group g is stable with m1 leader and m2 follower
    And the SyncGroup request proposes new-L and new-F assignments with identity
      | member | instance | generation_delta | protocol_type | protocol_name |
      | m1 | absent | 0 | absent | absent |
    When the coordinator handles the classic SyncGroup request
    Then SyncGroup returns read-L and leaves group state stable changing assignments only on leader installation
  Scenario: Matching protocol fields preserve the current assignment
    Given classic group g is stable with m1 leader and m2 follower
    And the SyncGroup request proposes new-L and new-F assignments with identity
      | member | instance | generation_delta | protocol_type | protocol_name |
      | m2 | absent | 0 | consumer | range |
    When the coordinator handles the classic SyncGroup request
    Then SyncGroup returns read-F and leaves group state stable changing assignments only on leader installation
  Scenario: Another protocol type refuses leader installation
    Given classic group g is completing with m1 leader and m2 follower
    And the SyncGroup request proposes new-L and new-F assignments with identity
      | member | instance | generation_delta | protocol_type | protocol_name |
      | m1 | absent | 0 | connect | absent |
    When the coordinator handles the classic SyncGroup request
    Then SyncGroup returns inconsistent-protocol and leaves group state completing changing assignments only on leader installation
  Scenario: Another protocol name is refused
    Given classic group g is stable with m1 leader and m2 follower
    And the SyncGroup request proposes new-L and new-F assignments with identity
      | member | instance | generation_delta | protocol_type | protocol_name |
      | m2 | absent | 0 | absent | roundrobin |
    When the coordinator handles the classic SyncGroup request
    Then SyncGroup returns inconsistent-protocol and leaves group state stable changing assignments only on leader installation
  Scenario: Generation mismatch takes precedence over protocol mismatch
    Given classic group g is stable with m1 leader and m2 follower
    And the SyncGroup request proposes new-L and new-F assignments with identity
      | member | instance | generation_delta | protocol_type | protocol_name |
      | m2 | absent | 1 | connect | absent |
    When the coordinator handles the classic SyncGroup request
    Then SyncGroup returns illegal-generation and leaves group state stable changing assignments only on leader installation
  Scenario: Unknown members are refused
    Given classic group g is stable with m1 leader and m2 follower
    And the SyncGroup request proposes new-L and new-F assignments with identity
      | member | instance | generation_delta | protocol_type | protocol_name |
      | ghost | absent | 0 | absent | absent |
    When the coordinator handles the classic SyncGroup request
    Then SyncGroup returns unknown-member and leaves group state stable changing assignments only on leader installation
  Scenario: Unknown static instances are refused
    Given classic group g is preparing with m1 leader and m2 follower
    And the SyncGroup request proposes new-L and new-F assignments with identity
      | member | instance | generation_delta | protocol_type | protocol_name |
      | m1 | i-unknown | 0 | absent | absent |
    When the coordinator handles the classic SyncGroup request
    Then SyncGroup returns unknown-member and leaves group state preparing changing assignments only on leader installation
