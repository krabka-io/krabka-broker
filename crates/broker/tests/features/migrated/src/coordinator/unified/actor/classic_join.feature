Feature: Classic JoinGroup admission and static membership in consumer groups

  Scenario: An empty consumer group becomes classic
    Given consumer group g subscribes to topic t with 0 native members and migration policy disabled
    When a dynamic classic member joins g without a member id using the range subscription to t
    Then the classic join returns error 79 with consumer tombstone true and classic group true

  Scenario: A live consumer group refuses classic members when migration is disabled
    Given consumer group g subscribes to topic t with 1 native members and migration policy disabled
    When a dynamic classic member joins g without a member id using the range subscription to t
    Then the classic join returns error 23 with consumer tombstone false and classic group false

  Scenario: Bidirectional migration admits a dynamic classic member through member-id allocation
    Given consumer group g subscribes to topic t with 1 native members and migration policy bidirectional
    When a dynamic classic member joins g without a member id using the range subscription to t
    Then the classic join returns error 79 with consumer tombstone false and classic group false

  Scenario: An assigned static member cannot claim an unheld instance
    Given consumer group g has native member native and a hosted classic member holding instance-1 under bidirectional migration
    When the static classic member rejoins at version 9 using member identity assigned and instance id instance-2
    Then the static rejoin returns only error 25 and leaves the log and both member identities unchanged

  Scenario: Another member cannot claim the held static instance
    Given consumer group g has native member native and a hosted classic member holding instance-1 under bidirectional migration
    When the static classic member rejoins at version 9 using member identity other and instance id instance-1
    Then the static rejoin returns only error 82 and leaves the log and both member identities unchanged
