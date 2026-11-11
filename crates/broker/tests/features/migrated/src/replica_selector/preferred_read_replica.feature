# KIP-392: the partition leader answers a consumer Fetch that carries a
# client.rack with a preferred read replica, or with -1 to keep the read on
# the leader. The rack-aware rule is Kafka's RackAwareReplicaSelector, after
# ReplicaManager.findPreferredReadReplica drops a selected leader. The most
# caught-up follower has the higher log end, then the shorter time since it
# caught up, then the higher node id.
Feature: The partition leader names a preferred read replica in the consumer's rack

  Rule: The leader selector keeps every read on the leader

    Scenario: A same-rack follower does not move a read off the leader
      Given the partition led by node 1 has these replicas
        | node | rack |
        | 1    | a    |
        | 2    | b    |
      When a consumer in rack b fetches through the leader selector
      Then the fetch names no preferred read replica

  Rule: A consumer that sends no rack reads from the leader

    Scenario: A consumer without a rack reads from the leader
      Given the partition led by node 1 has these replicas
        | node | rack |
        | 1    | a    |
        | 2    | b    |
      When a consumer that sends no rack fetches through the rack-aware selector
      Then the fetch names no preferred read replica

    Scenario: An empty rack is no rack, even beside a follower with an empty rack
      Given the partition led by node 1 has these replicas
        | node | rack  |
        | 1    | a     |
        | 2    | empty |
        | 3    | none  |
      When a consumer that sends an empty rack fetches through the rack-aware selector
      Then the fetch names no preferred read replica

  Rule: A leader in the consumer's rack keeps the read, whatever its followers

    Scenario: A same-rack follower with a lower id does not outrank the leader
      Given the partition led by node 2 has these replicas
        | node | rack |
        | 1    | b    |
        | 2    | b    |
      When a consumer in rack b fetches through the rack-aware selector
      Then the fetch names no preferred read replica

    Scenario: A same-rack follower further ahead does not outrank the leader
      Given the partition led by node 2 has these replicas
        | node | rack | log end |
        | 1    | b    | 500     |
        | 2    | b    | 100     |
      When a consumer in rack b fetches through the rack-aware selector
      Then the fetch names no preferred read replica

    Scenario: The leader alone in the consumer's rack keeps the read
      Given the partition led by node 1 has these replicas
        | node | rack |
        | 1    | b    |
        | 2    | a    |
      When a consumer in rack b fetches through the rack-aware selector
      Then the fetch names no preferred read replica

  Rule: Only an in-sync follower that serves clients is a candidate

    Scenario: An in-sync follower in the consumer's rack takes the read
      Given the partition led by node 1 has these replicas
        | node | rack |
        | 1    | a    |
        | 2    | b    |
      When a consumer in rack b fetches through the rack-aware selector
      Then the fetch prefers node 2

    Scenario: No replica in the consumer's rack keeps the read on the leader
      Given the partition led by node 1 has these replicas
        | node | rack |
        | 1    | a    |
        | 2    | a    |
      When a consumer in rack z fetches through the rack-aware selector
      Then the fetch names no preferred read replica

    Scenario: A same-rack follower out of the ISR is no candidate
      Given the partition led by node 1 has these replicas
        | node | rack | in sync |
        | 1    | a    | -       |
        | 2    | b    | false   |
      When a consumer in rack b fetches through the rack-aware selector
      Then the fetch names no preferred read replica

    Scenario: An out-of-sync follower loses to an in-sync one however far ahead it is
      Given the partition led by node 1 has these replicas
        | node | rack | in sync | log end |
        | 1    | a    | -       | -       |
        | 2    | b    | false   | 999     |
        | 3    | b    | -       | -       |
      When a consumer in rack b fetches through the rack-aware selector
      Then the fetch prefers node 3

    Scenario: A witness in the consumer's rack is no candidate
      Given the partition led by node 1 has these replicas
        | node | rack | witness |
        | 1    | a    | -       |
        | 2    | b    | true    |
      When a consumer in rack b fetches through the rack-aware selector
      Then the fetch names no preferred read replica

    Scenario: A same-rack follower beats a witness further ahead
      Given the partition led by node 1 has these replicas
        | node | rack | witness | log end | caught up ms ago |
        | 1    | a    | -       | -       | -                |
        | 2    | b    | true    | -       | -                |
        | 3    | b    | -       | 50      | 9000             |
      When a consumer in rack b fetches through the rack-aware selector
      Then the fetch prefers node 3

  Rule: The most caught-up same-rack follower wins

    Scenario: The higher log end wins
      Given the partition led by node 1 has these replicas
        | node | rack | log end |
        | 1    | a    | -       |
        | 2    | b    | 90      |
        | 3    | b    | -       |
      When a consumer in rack b fetches through the rack-aware selector
      Then the fetch prefers node 3

    Scenario: The higher log end wins over a shorter time since caught up
      Given the partition led by node 1 has these replicas
        | node | rack | log end | caught up ms ago |
        | 1    | a    | -       | -                |
        | 2    | b    | 95      | 5000             |
        | 3    | b    | 90      | -                |
      When a consumer in rack b fetches through the rack-aware selector
      Then the fetch prefers node 2

    Scenario: The shorter time since caught up breaks a log end tie
      Given the partition led by node 1 has these replicas
        | node | rack | caught up ms ago |
        | 1    | a    | -                |
        | 2    | b    | 5000             |
        | 3    | b    | 100              |
      When a consumer in rack b fetches through the rack-aware selector
      Then the fetch prefers node 3

    Scenario: A follower that never caught up loses a log end tie
      Given the partition led by node 1 has these replicas
        | node | rack | caught up ms ago |
        | 1    | a    | -                |
        | 2    | b    | never            |
        | 3    | b    | 60000            |
      When a consumer in rack b fetches through the rack-aware selector
      Then the fetch prefers node 3

    Scenario: The higher node id breaks a full tie
      Given the partition led by node 1 has these replicas
        | node | rack |
        | 1    | a    |
        | 2    | b    |
        | 4    | b    |
        | 3    | b    |
      When a consumer in rack b fetches through the rack-aware selector
      Then the fetch prefers node 4
