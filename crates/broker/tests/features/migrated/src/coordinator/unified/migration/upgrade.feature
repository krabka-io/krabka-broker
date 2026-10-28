Feature: Classic consumer online upgrade validates persisted protocol compatibility
  Scenario: A member that has never synced can upgrade
    Given classic consumer group g has these persisted member subscriptions and assignments
      | subscription | assignment | userdata |
      | t1 | absent | null |
    When the coordinator converts the classic group to consumer membership
    Then online conversion is allowed with the exact Kafka reason
  Scenario: An empty assignment can upgrade
    Given classic consumer group g has these persisted member subscriptions and assignments
      | subscription | assignment | userdata |
      | t1 | empty | null |
    When the coordinator converts the classic group to consumer membership
    Then online conversion is allowed with the exact Kafka reason
  Scenario: A standard assignment without user data can upgrade
    Given classic consumer group g has these persisted member subscriptions and assignments
      | subscription | assignment | userdata |
      | t1 | 0 | null |
    When the coordinator converts the classic group to consumer membership
    Then online conversion is allowed with the exact Kafka reason
  Scenario: Empty custom user data is harmless
    Given classic consumer group g has these persisted member subscriptions and assignments
      | subscription | assignment | userdata |
      | t1 | 1 | empty |
    When the coordinator converts the classic group to consumer membership
    Then online conversion is allowed with the exact Kafka reason
  Scenario: Assignments above the schema version decode at the highest supported version
    Given classic consumer group g has these persisted member subscriptions and assignments
      | subscription | assignment | userdata |
      | t1 | 9 | null |
    When the coordinator converts the classic group to consumer membership
    Then online conversion is allowed with the exact Kafka reason
  Scenario: A malformed later subscription refuses conversion
    Given classic consumer group g has these persisted member subscriptions and assignments
      | subscription | assignment | userdata |
      | t1 | absent | null |
      | malformed-negative | absent | null |
    When the coordinator converts the classic group to consumer membership
    Then online conversion is malformed with the exact Kafka reason
  Scenario: A malformed assignment refuses conversion
    Given classic consumer group g has these persisted member subscriptions and assignments
      | subscription | assignment | userdata |
      | t1 | malformed | null |
    When the coordinator converts the classic group to consumer membership
    Then online conversion is malformed with the exact Kafka reason
  Scenario: Custom assignor user data refuses conversion
    Given classic consumer group g has these persisted member subscriptions and assignments
      | subscription | assignment | userdata |
      | t1 | 0 | sticky |
    When the coordinator converts the classic group to consumer membership
    Then online conversion is custom-assignor with the exact Kafka reason
  Scenario: Earlier custom assignor data takes precedence over a later malformed subscription
    Given classic consumer group g has these persisted member subscriptions and assignments
      | subscription | assignment | userdata |
      | t1 | 0 | sticky |
      | malformed-zero | absent | null |
    When the coordinator converts the classic group to consumer membership
    Then online conversion is custom-assignor with the exact Kafka reason
  Scenario Outline: Consumer subscription version handling follows Kafka
    Given a consumer protocol subscription has encoding <encoding>
    When the coordinator decodes the embedded consumer subscription
    Then the embedded subscription resolves to <topics>
    Examples:
      | encoding | topics |
      | empty | undecodable |
      | short | undecodable |
      | negative-version | undecodable |
      | above-schema | t1 |
  Scenario Outline: Online policy protocol and size checks use Kafka refusal messages
    Given classic group g uses protocol <protocol> with <members> consumer members
    When the coordinator validates online upgrade enabled <enabled> with maximum size <maximum>
    Then online conversion is <outcome> with the exact Kafka reason
    Examples:
      | protocol | members | enabled | maximum | outcome |
      | consumer | 2 | true | 2 | allowed |
      | consumer | 2 | false | 2 | disabled |
      | connect | 2 | true | 2 | wrong-protocol |
      | consumer | 3 | true | 2 | too-large |
