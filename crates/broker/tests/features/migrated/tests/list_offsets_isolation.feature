Feature: ListOffsets obeys acknowledged and stable bounds for both isolation levels
  Scenario: Both isolation levels stop LATEST at the acknowledged high watermark
    Given a standalone broker is running for isolation bound queries
    And topic list-offsets-isolation-unreplicated has the following settled nontransactional records
      | value | timestamp |
      | a | auto |
      | b | auto |
      | c | auto |
      | d | auto |
    When LATEST is requested at both isolation levels
    Then the complete LATEST rows report uncommitted offset 4 and committed offset 4
    When the high watermark is held at 2 while both isolation levels are queried with at most 20 attempts
    Then every watermark hold targeted a partition actually hosted by the broker
    And the complete LATEST rows report uncommitted offset 2 and committed offset 2

  Scenario Outline: Commit and abort both release the last stable offset past their marker
    Given a standalone broker is running for isolation bound queries
    And the standalone broker's transaction coordinator is ready
    And topic <topic> has the following settled nontransactional records
      | value | timestamp |
      | settled-a | auto |
      | settled-b | auto |
    When LATEST is requested at both isolation levels
    Then the complete LATEST rows report uncommitted offset 2 and committed offset 2
    When transactional producer <tid> opens a transaction and appends these records
      | value | timestamp |
      | open-c | auto |
      | open-d | auto |
    And LATEST is requested at both isolation levels
    Then the complete LATEST rows report uncommitted offset 4 and committed offset 2
    When the open transaction resolves by <resolution> and its control marker settles at log end 5
    And LATEST is requested at both isolation levels
    Then the complete LATEST rows report uncommitted offset 5 and committed offset 5
    Examples:
      | resolution | topic | tid |
      | commit | list-offsets-isolation-commit | list-offsets-isolation-commit-tid |
      | abort | list-offsets-isolation-abort | list-offsets-isolation-abort-tid |

  Scenario: Open transactions fence timestamp matches at or beyond the stable offset
    Given a standalone broker is running for isolation bound queries
    And the standalone broker's transaction coordinator is ready
    And topic list-offsets-isolation-timestamps has the following settled nontransactional records
      | value | timestamp |
      | settled-a | 1000 |
      | settled-b | 1100 |
    When transactional producer list-offsets-isolation-timestamps-tid opens a transaction and appends these records
      | value | timestamp |
      | open-c | 5000 |
      | open-d | 5100 |
    And timestamp selection -3 is requested at both isolation levels
    Then the complete selected rows contain these offsets and timestamps with no errors
      | isolation | offset | timestamp |
      | read_uncommitted | 3 | 5100 |
      | read_committed | -1 | -1 |
    When timestamp selection 5000 is requested at both isolation levels
    Then the complete selected rows contain these offsets and timestamps with no errors
      | isolation | offset | timestamp |
      | read_uncommitted | 2 | 5000 |
      | read_committed | -1 | -1 |
    When timestamp selection 1000 is requested at both isolation levels
    Then the complete selected rows contain these offsets and timestamps with no errors
      | isolation | offset | timestamp |
      | read_uncommitted | 0 | 1000 |
      | read_committed | 0 | 1000 |
    When the open transaction resolves by commit and its control marker settles at log end 5
    And these timestamp selections are requested after the transaction resolves
      | timestamp |
      | -3 |
      | 5000 |
      | 1000 |
    Then all post resolution selections agree between isolation levels and resolve actual records
