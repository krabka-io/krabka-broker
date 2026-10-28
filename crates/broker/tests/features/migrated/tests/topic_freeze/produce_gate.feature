Feature: Topic freeze rejects writes within its scope
  Scenario: a literal freeze refuses produce and the control topic still accepts
    Given a broker with unfrozen topics orders,control
    When the client writes one record to orders
    Then the write to orders is accepted and its log end is 1
    When the client writes one record to control
    Then the write to control is accepted and its log end is 1
    When the operator freezes literal scope orders because DR cutover
    And the client writes one record to orders
    Then the write to orders is refused and its log end stays 1
    When the client writes one record to control
    Then the write to control is accepted and its log end is 2

  Scenario: a prefix freeze refuses produce to every topic it covers
    Given a broker with unfrozen topics tenant-a.orders,tenant-b.orders,control
    When the client writes one record to tenant-a.orders
    Then the write to tenant-a.orders is accepted and its log end is 1
    When the operator freezes prefixed scope tenant-a. because offboarding
    And the client writes one record to tenant-a.orders
    Then the write to tenant-a.orders is refused and its log end stays 1
    When the client writes one record to tenant-b.orders
    Then the write to tenant-b.orders is accepted and its log end is 1
    When the client writes one record to control
    Then the write to control is accepted and its log end is 1

  Scenario: a topic created after a covering prefix freeze is frozen on arrival
    Given a broker with unfrozen topics control
    When the operator freezes prefixed scope tenant-a. because pre-restore
    And topic tenant-a.late is created after the freeze
    And the client writes one record to tenant-a.late
    Then the write to tenant-a.late is refused and its log end stays 0
    When the client writes one record to control
    Then the write to control is accepted and its log end is 1
