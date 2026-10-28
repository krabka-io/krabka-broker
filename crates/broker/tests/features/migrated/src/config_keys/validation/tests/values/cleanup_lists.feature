Feature: Kafka cleanup lists preserve order-insensitive membership and list validation
  Empty lists disable cleanup; duplicate, empty-item, and unknown members remain invalid.

  Scenario Outline: Kafka cleanup lists preserve order-insensitive membership and list validation
    Given a topic alteration supplies key cleanup.policy and quoted value <value>
    When the cleanup policy parser and topic validator read the supplied list
    Then cleanup resolves as <policy> and the validator agrees with that outcome

    Examples:
      | value | policy |
      | "delete" | delete |
      | "compact" | compact |
      | "compact,delete" | compact-and-delete |
      | "delete,compact" | compact-and-delete |
      | "compact, delete" | compact-and-delete |
      | "junk" | invalid |
      | "compact,junk" | invalid |
      | "compact," | invalid |
      | "delete," | invalid |
      | "delete,delete" | invalid |
      | "compact, compact" | invalid |
      | "" | no-cleanup |
      | " " | no-cleanup |
