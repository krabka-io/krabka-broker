Feature: Share state append failures map to Kafka operation errors
  The response code reflects coordinator availability while the message retains
  the original partition or timeout failure.

  Scenario Outline: Map a failed append to state partition 3
    Given share state partition 3 rejects append because it is <condition>
    When the coordinator maps the append failure to a share state operation error
    Then the share state operation error has code <code> and message <message>

    Examples:
      | condition | code | message |
      | awaiting replay | 14 | The coordinator is loading and hence can't process requests. |
      | not local | 15 | This server does not host this topic-partition. |
      | not the leader | 16 | This is not the correct coordinator. |

  Scenario: A timed-out append reports unavailable coordinator with timeout text
    Given the share state append timed out
    When the coordinator maps the append failure to a share state operation error
    Then the share state operation error has code 15 and message The request timed out.
