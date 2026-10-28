Feature: Kafka retention cuts and ordered segment deletion
  Local deletion performs size and time passes in order, including a nonempty
  active segment when warranted. Remote deletion charges time removals against
  size debt while floor breaches do not pay that debt. Blocked segments stop passes.

  Scenario Outline: Retained barrier cuts expire at the inclusive cut boundary
    Given published barrier cut <published> with <retained> retained cuts
    When held barrier cut <held> is evaluated for expiry
    Then the held barrier cut has expired: <expired>

    Examples:
      | boundary | published | retained | held | expired |
      | before expiry | 10 | 3 | 6 | true |
      | at expiry | 10 | 3 | 7 | true |
      | after expiry | 10 | 3 | 8 | false |
      | no cuts | 10 | 0 | minimum | false |
      | negative cuts | 10 | -1 | minimum | false |
      | minimum published | minimum | 1 | minimum | false |
      | minimum successor | minimum-plus-one | 1 | minimum | true |
      | maximum published | maximum | 2147483647 | maximum | false |

  Scenario: Local retention - no segments
    Given a local retention size debt of 100 bytes
    And these local segments are ordered oldest first:
      | size | blocked | expired |
    When Kafka local retention applies its ordered deletion passes
    Then the oldest 0 local segments are removed

  Scenario: Local retention - no pressure keeps everything
    Given a local retention size debt of none bytes
    And these local segments are ordered oldest first:
      | size | blocked | expired |
      | 10 | false | false |
      | 10 | false | false |
    When Kafka local retention applies its ordered deletion passes
    Then the oldest 0 local segments are removed

  Scenario: Local retention - a debt of 15 over three 10-byte segments deletes one, not two
    Given a local retention size debt of 15 bytes
    And these local segments are ordered oldest first:
      | size | blocked | expired |
      | 10 | false | false |
      | 10 | false | false |
      | 10 | false | false |
    When Kafka local retention applies its ordered deletion passes
    Then the oldest 1 local segments are removed

  Scenario: Local retention - a debt of exactly two segments deletes two
    Given a local retention size debt of 20 bytes
    And these local segments are ordered oldest first:
      | size | blocked | expired |
      | 10 | false | false |
      | 10 | false | false |
      | 10 | false | false |
    When Kafka local retention applies its ordered deletion passes
    Then the oldest 2 local segments are removed

  Scenario: Local retention - a debt below the oldest segment deletes nothing
    Given a local retention size debt of 5 bytes
    And these local segments are ordered oldest first:
      | size | blocked | expired |
      | 10 | false | false |
      | 1 | false | false |
    When Kafka local retention applies its ordered deletion passes
    Then the oldest 0 local segments are removed

  Scenario: Local retention - a zero debt still deletes leading empty segments
    Given a local retention size debt of 0 bytes
    And these local segments are ordered oldest first:
      | size | blocked | expired |
      | 0 | false | false |
      | 0 | false | false |
      | 10 | false | false |
    When Kafka local retention applies its ordered deletion passes
    Then the oldest 2 local segments are removed

  Scenario: Local retention - no size pass keeps empty segments
    Given a local retention size debt of none bytes
    And these local segments are ordered oldest first:
      | size | blocked | expired |
      | 0 | false | false |
      | 10 | false | false |
    When Kafka local retention applies its ordered deletion passes
    Then the oldest 0 local segments are removed

  Scenario: Local retention - time pass deletes the expired prefix
    Given a local retention size debt of none bytes
    And these local segments are ordered oldest first:
      | size | blocked | expired |
      | 10 | false | true |
      | 10 | false | true |
      | 10 | false | false |
      | 10 | false | true |
    When Kafka local retention applies its ordered deletion passes
    Then the oldest 2 local segments are removed

  Scenario: Local retention - the size pass ends at the first misfit and never resumes
    Given a local retention size debt of 5 bytes
    And these local segments are ordered oldest first:
      | size | blocked | expired |
      | 10 | false | true |
      | 1 | false | false |
    When Kafka local retention applies its ordered deletion passes
    Then the oldest 1 local segments are removed

  Scenario: Local retention - the size pass runs first and the time pass continues after it
    Given a local retention size debt of 12 bytes
    And these local segments are ordered oldest first:
      | size | blocked | expired |
      | 10 | false | false |
      | 10 | false | true |
      | 1 | false | false |
    When Kafka local retention applies its ordered deletion passes
    Then the oldest 2 local segments are removed

  Scenario: Local retention - an expired segment that fits is charged to the debt
    Given a local retention size debt of 10 bytes
    And these local segments are ordered oldest first:
      | size | blocked | expired |
      | 3 | false | true |
      | 10 | false | false |
    When Kafka local retention applies its ordered deletion passes
    Then the oldest 1 local segments are removed

  Scenario: Local retention - the size pass reaches past an expired prefix
    Given a local retention size debt of 6 bytes
    And these local segments are ordered oldest first:
      | size | blocked | expired |
      | 3 | false | true |
      | 3 | false | false |
      | 10 | false | false |
    When Kafka local retention applies its ordered deletion passes
    Then the oldest 2 local segments are removed

  Scenario: Local retention - a blocked segment stops every pass
    Given a local retention size debt of 100 bytes
    And these local segments are ordered oldest first:
      | size | blocked | expired |
      | 10 | false | true |
      | 10 | true | true |
      | 10 | false | true |
    When Kafka local retention applies its ordered deletion passes
    Then the oldest 1 local segments are removed

  Scenario: Local retention - a blocked oldest segment deletes nothing
    Given a local retention size debt of 100 bytes
    And these local segments are ordered oldest first:
      | size | blocked | expired |
      | 10 | true | true |
      | 10 | false | true |
    When Kafka local retention applies its ordered deletion passes
    Then the oldest 0 local segments are removed

  Scenario: Local retention - an expired log goes whole, the active segment included
    Given a local retention size debt of none bytes
    And these local segments are ordered oldest first:
      | size | blocked | expired |
      | 10 | false | true |
      | 10 | false | true |
    When Kafka local retention applies its ordered deletion passes
    Then the oldest 2 local segments are removed

  Scenario: Local retention - a debt that covers the whole log takes the active segment too
    Given a local retention size debt of 20 bytes
    And these local segments are ordered oldest first:
      | size | blocked | expired |
      | 10 | false | false |
      | 10 | false | false |
    When Kafka local retention applies its ordered deletion passes
    Then the oldest 2 local segments are removed

  Scenario: Local retention - an empty active segment is never deleted
    Given a local retention size debt of none bytes
    And these local segments are ordered oldest first:
      | size | blocked | expired |
      | 10 | false | true |
      | 0 | false | true |
    When Kafka local retention applies its ordered deletion passes
    Then the oldest 1 local segments are removed

  Scenario: Local retention - an empty active segment stays under any debt
    Given a local retention size debt of maximum bytes
    And these local segments are ordered oldest first:
      | size | blocked | expired |
      | 10 | false | false |
      | 0 | false | false |
    When Kafka local retention applies its ordered deletion passes
    Then the oldest 1 local segments are removed

  Scenario: Local retention - a lone empty segment stays
    Given a local retention size debt of 0 bytes
    And these local segments are ordered oldest first:
      | size | blocked | expired |
      | 0 | false | true |
    When Kafka local retention applies its ordered deletion passes
    Then the oldest 0 local segments are removed

  Scenario: Local retention - a lone active segment that breaches goes
    Given a local retention size debt of none bytes
    And these local segments are ordered oldest first:
      | size | blocked | expired |
      | 1 | false | true |
    When Kafka local retention applies its ordered deletion passes
    Then the oldest 1 local segments are removed

  Scenario: Local retention - the full u64 range fits without overflow
    Given a local retention size debt of maximum bytes
    And these local segments are ordered oldest first:
      | size | blocked | expired |
      | 18446744073709551615 | false | false |
      | 1 | false | false |
    When Kafka local retention applies its ordered deletion passes
    Then the oldest 1 local segments are removed

  Scenario: Remote retention - no segments
    Given a remote retention size debt of 100 bytes
    And these remote segments are ordered oldest first:
      | size | blocked | floor_breached | time_expired |
    When Kafka remote retention evaluates delete-enabled and delete-disabled tiers
    Then the delete-enabled tier removes 0 oldest remote segments and the delete-disabled tier removes none

  Scenario: Remote retention - no pressure keeps everything
    Given a remote retention size debt of 0 bytes
    And these remote segments are ordered oldest first:
      | size | blocked | floor_breached | time_expired |
      | 10 | false | false | false |
      | 10 | false | false | false |
    When Kafka remote retention evaluates delete-enabled and delete-disabled tiers
    Then the delete-enabled tier removes 0 oldest remote segments and the delete-disabled tier removes none

  Scenario: Remote retention - a debt of 15 over three 10-byte segments deletes one, not two
    Given a remote retention size debt of 15 bytes
    And these remote segments are ordered oldest first:
      | size | blocked | floor_breached | time_expired |
      | 10 | false | false | false |
      | 10 | false | false | false |
      | 10 | false | false | false |
    When Kafka remote retention evaluates delete-enabled and delete-disabled tiers
    Then the delete-enabled tier removes 1 oldest remote segments and the delete-disabled tier removes none

  Scenario: Remote retention - a debt of exactly two segments deletes two
    Given a remote retention size debt of 20 bytes
    And these remote segments are ordered oldest first:
      | size | blocked | floor_breached | time_expired |
      | 10 | false | false | false |
      | 10 | false | false | false |
      | 10 | false | false | false |
    When Kafka remote retention evaluates delete-enabled and delete-disabled tiers
    Then the delete-enabled tier removes 2 oldest remote segments and the delete-disabled tier removes none

  Scenario: Remote retention - a zero debt never deletes an empty segment
    Given a remote retention size debt of 0 bytes
    And these remote segments are ordered oldest first:
      | size | blocked | floor_breached | time_expired |
      | 0 | false | false | false |
    When Kafka remote retention evaluates delete-enabled and delete-disabled tiers
    Then the delete-enabled tier removes 0 oldest remote segments and the delete-disabled tier removes none

  Scenario: Remote retention - a positive debt deletes an empty segment
    Given a remote retention size debt of 5 bytes
    And these remote segments are ordered oldest first:
      | size | blocked | floor_breached | time_expired |
      | 0 | false | false | false |
      | 10 | false | false | false |
    When Kafka remote retention evaluates delete-enabled and delete-disabled tiers
    Then the delete-enabled tier removes 1 oldest remote segments and the delete-disabled tier removes none

  Scenario: Remote retention - the time axis stops at the first segment in the window
    Given a remote retention size debt of 0 bytes
    And these remote segments are ordered oldest first:
      | size | blocked | floor_breached | time_expired |
      | 10 | false | false | true |
      | 10 | false | false | true |
      | 10 | false | false | false |
      | 10 | false | false | true |
    When Kafka remote retention evaluates delete-enabled and delete-disabled tiers
    Then the delete-enabled tier removes 2 oldest remote segments and the delete-disabled tier removes none

  Scenario: Remote retention - a time deletion lowers the debt, never below zero
    Given a remote retention size debt of 20 bytes
    And these remote segments are ordered oldest first:
      | size | blocked | floor_breached | time_expired |
      | 30 | false | false | true |
      | 1 | false | false | false |
    When Kafka remote retention evaluates delete-enabled and delete-disabled tiers
    Then the delete-enabled tier removes 1 oldest remote segments and the delete-disabled tier removes none

  Scenario: Remote retention - size resumes after a time deletion charged against the debt
    Given a remote retention size debt of 20 bytes
    And these remote segments are ordered oldest first:
      | size | blocked | floor_breached | time_expired |
      | 10 | false | false | true |
      | 5 | false | false | false |
      | 10 | false | false | false |
    When Kafka remote retention evaluates delete-enabled and delete-disabled tiers
    Then the delete-enabled tier removes 2 oldest remote segments and the delete-disabled tier removes none

  Scenario: Remote retention - a log-start breach leaves the debt alone
    Given a remote retention size debt of 10 bytes
    And these remote segments are ordered oldest first:
      | size | blocked | floor_breached | time_expired |
      | 10 | false | true | false |
      | 10 | false | false | false |
      | 10 | false | false | false |
    When Kafka remote retention evaluates delete-enabled and delete-disabled tiers
    Then the delete-enabled tier removes 2 oldest remote segments and the delete-disabled tier removes none

  Scenario: Remote retention - the breach covers what the time axis would stop at
    Given a remote retention size debt of 0 bytes
    And these remote segments are ordered oldest first:
      | size | blocked | floor_breached | time_expired |
      | 10 | false | true | false |
      | 10 | false | false | true |
      | 10 | false | false | false |
    When Kafka remote retention evaluates delete-enabled and delete-disabled tiers
    Then the delete-enabled tier removes 2 oldest remote segments and the delete-disabled tier removes none

  Scenario: Remote retention - the breach alone takes every breached segment
    Given a remote retention size debt of 0 bytes
    And these remote segments are ordered oldest first:
      | size | blocked | floor_breached | time_expired |
      | 10 | false | true | false |
      | 10 | false | true | false |
      | 10 | false | true | false |
    When Kafka remote retention evaluates delete-enabled and delete-disabled tiers
    Then the delete-enabled tier removes 3 oldest remote segments and the delete-disabled tier removes none
