Feature: Kafka ISR shrink and expansion eligibility
  Current ISR members may retain recently earned catch-up credit. Admission of an
  out-of-sync member additionally requires the committed prefix, the leader epoch
  start, and an eligible broker incarnation.

  Background:
    Given an ISR leader with log end 120 and committed high watermark 100

  Scenario: The leader remains in the ISR
    Given this replica is considered for ISR membership:
      | role | follower_end | recently_caught_up | leader_epoch_start | fenced |
      | leader | -1 | false | 90 | false |
    When the leader applies ISR shrink and expansion rules
    Then the candidate belongs in the ISR: true

  Scenario: A replica removed from assignment leaves the ISR
    Given this replica is considered for ISR membership:
      | role | follower_end | recently_caught_up | leader_epoch_start | fenced |
      | unassigned | 120 | true | 90 | false |
    When the leader applies ISR shrink and expansion rules
    Then the candidate belongs in the ISR: false

  Scenario: An idle follower at leader end remains in the ISR
    Given this replica is considered for ISR membership:
      | role | follower_end | recently_caught_up | leader_epoch_start | fenced |
      | in-sync | 120 | false | 90 | false |
    When the leader applies ISR shrink and expansion rules
    Then the candidate belongs in the ISR: true

  Scenario: Recently caught-up members remain in sync
    Given this replica is considered for ISR membership:
      | role | follower_end | recently_caught_up | leader_epoch_start | fenced |
      | in-sync | 50 | true | 90 | false |
    When the leader applies ISR shrink and expansion rules
    Then the candidate belongs in the ISR: true

  Scenario: A follower lagging beyond the bound leaves
    Given this replica is considered for ISR membership:
      | role | follower_end | recently_caught_up | leader_epoch_start | fenced |
      | in-sync | 119 | false | 90 | false |
    When the leader applies ISR shrink and expansion rules
    Then the candidate belongs in the ISR: false

  Scenario: An unfetched in-sync follower leaves after the bound
    Given this replica is considered for ISR membership:
      | role | follower_end | recently_caught_up | leader_epoch_start | fenced |
      | in-sync | -1 | false | 90 | false |
    When the leader applies ISR shrink and expansion rules
    Then the candidate belongs in the ISR: false

  Scenario: A follower reaching the committed watermark may join
    Given this replica is considered for ISR membership:
      | role | follower_end | recently_caught_up | leader_epoch_start | fenced |
      | out-of-sync | 100 | false | 90 | false |
    When the leader applies ISR shrink and expansion rules
    Then the candidate belongs in the ISR: true

  Scenario: A follower one offset short of commitment stays out
    Given this replica is considered for ISR membership:
      | role | follower_end | recently_caught_up | leader_epoch_start | fenced |
      | out-of-sync | 99 | true | 90 | false |
    When the leader applies ISR shrink and expansion rules
    Then the candidate belongs in the ISR: false

  Scenario: A follower before the leader epoch start stays out
    Given this replica is considered for ISR membership:
      | role | follower_end | recently_caught_up | leader_epoch_start | fenced |
      | out-of-sync | 105 | true | 110 | false |
    When the leader applies ISR shrink and expansion rules
    Then the candidate belongs in the ISR: false

  Scenario: Unknown leader epoch start prevents admission
    Given this replica is considered for ISR membership:
      | role | follower_end | recently_caught_up | leader_epoch_start | fenced |
      | out-of-sync | 120 | true | unknown | false |
    When the leader applies ISR shrink and expansion rules
    Then the candidate belongs in the ISR: false

  Scenario: A fenced follower cannot join even at log end
    Given this replica is considered for ISR membership:
      | role | follower_end | recently_caught_up | leader_epoch_start | fenced |
      | out-of-sync | 120 | true | 90 | true |
    When the leader applies ISR shrink and expansion rules
    Then the candidate belongs in the ISR: false

  Scenario: An out-of-sync follower that never fetched stays out
    Given this replica is considered for ISR membership:
      | role | follower_end | recently_caught_up | leader_epoch_start | fenced |
      | out-of-sync | -1 | true | 90 | false |
    When the leader applies ISR shrink and expansion rules
    Then the candidate belongs in the ISR: false
