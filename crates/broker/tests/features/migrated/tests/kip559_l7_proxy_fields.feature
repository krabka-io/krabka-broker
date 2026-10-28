Feature: Group responses identify their negotiated protocol for stateless proxies
  Scenario: JoinGroup exposes its negotiated protocol to a stateless proxy
    Given a ready coordinator for proxy-visible group negotiation
    When a member joins the consumer range group
    Then membership bootstrap supplies a nonempty member id
    And successful JoinGroup identifies consumer and range

  Scenario: SyncGroup exposes its negotiated protocol to a stateless proxy
    Given a ready coordinator for proxy-visible group negotiation
    When a member joins the consumer range group
    Then membership bootstrap supplies a nonempty member id
    And successful JoinGroup identifies consumer and range
    When the joined member synchronizes its assignment
    Then successful SyncGroup identifies consumer and range

  Scenario: A refused incompatible join does not advertise a negotiated protocol
    Given a ready coordinator for proxy-visible group negotiation
    When a member joins the consumer range group
    Then membership bootstrap supplies a nonempty member id
    And successful JoinGroup identifies consumer and range
    When another member tries to join using the stream protocol
    Then JoinGroup returns only INCONSISTENT_GROUP_PROTOCOL without protocol identity

  Scenario: A refused synchronization does not advertise a protocol or assignment
    Given a ready coordinator for proxy-visible group negotiation
    When a member joins the consumer range group
    Then membership bootstrap supplies a nonempty member id
    And successful JoinGroup identifies consumer and range
    When an unknown member tries to synchronize the current generation
    Then SyncGroup returns only UNKNOWN_MEMBER_ID without protocol identity or assignment
