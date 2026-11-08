Feature: Tiered segments sit under the configured key prefix
  An operator can copy a prefix from a console with stray spaces or slashes
  around it. Every krabka 1.x release before krabka.version 3 writes the
  prefix exactly as configured, and krabka.version 3 trims it. A segment
  written under either spelling stays readable and deletable at every level.

  Gherkin trims table cells, so a · in a prefix stands for a space.

  Rule: Until krabka.version 3 is finalized a segment is written under the prefix as configured

    Scenario Outline: A segment copied below krabka.version 3 keeps the configured spelling
      Given a remote tier with key prefix <prefix>
      And the cluster has finalized krabka.version <level>
      When a segment of orders partition 0 is copied to the remote tier
      Then the segment log is stored in its partition directory under <root>

      Examples:
        | case                          | level | prefix   | root     |
        | the 1.0.0 baseline            | 0     | ·tier/·  | ·tier/·  |
        | the first krabka level        | 1     | ·tier/·  | ·tier/·  |
        | the level just below          | 2     | ·/tier/· | ·/tier/· |
        | slashes alone always collapse | 2     | /tier/   | tier     |

  Rule: Once krabka.version 3 is finalized a segment is written under the trimmed prefix

    Scenario Outline: A segment copied at krabka.version 3 takes the trimmed prefix
      Given a remote tier with key prefix <prefix>
      And the cluster has finalized krabka.version 3
      When a segment of orders partition 0 is copied to the remote tier
      Then the segment log is stored in its partition directory under tier

      Examples:
        | case               | prefix   |
        | padded             | ·tier/·  |
        | slashed            | /tier/   |
        | padded and slashed | ·/tier/· |

    Scenario: A prefix of only spaces means no prefix at krabka.version 3
      Given a remote tier with key prefix ···
      And the cluster has finalized krabka.version 3
      When a segment of orders partition 0 is copied to the remote tier
      Then the segment log is stored in its partition directory at the bucket root

  Rule: A prefix with no stray characters is written the same way at every level

    Scenario Outline: A clean prefix keeps its keys across the finalization
      Given a remote tier with key prefix tier
      And the cluster has finalized krabka.version <level>
      When a segment of orders partition 0 is copied to the remote tier
      Then the segment log is stored in its partition directory under tier

      Examples:
        | case                | level |
        | before finalization | 2     |
        | after finalization  | 3     |

  Rule: A segment stays readable on both sides of the finalization

    Scenario Outline: A segment written under one spelling is fetched under the other
      Given a remote tier with key prefix ·tier·
      And a segment of orders partition 0 was copied to it at krabka.version <written>
      And the cluster has finalized krabka.version <read>
      When a consumer reads the segment back from the remote tier
      Then the remote tier returns the segment as it was copied

      Examples:
        | case                        | written | read |
        | written before finalization | 2       | 3    |
        | written after finalization  | 3       | 2    |

  Rule: Deleting a segment removes it under both spellings

    Scenario: Deleting after finalization removes a segment written before it
      Given a remote tier with key prefix ·tier·
      And a segment of orders partition 0 was copied to it at krabka.version 2
      And the cluster has finalized krabka.version 3
      When the remote log manager deletes the segment
      Then the remote tier holds no object of the segment
