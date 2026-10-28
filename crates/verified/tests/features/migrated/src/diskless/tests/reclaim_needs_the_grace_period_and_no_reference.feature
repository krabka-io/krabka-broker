Feature: src diskless tests reclaim needs the grace period and no reference

  Scenario Outline: retention prefix applies each kafka predicate and keeps the newest range
    Then diskless tests reclaim needs the grace period and no reference: retention prefix applies each kafka predicate and keeps the newest range for <example>

    Examples:
      | example                            |
      | no policy                          |
      | time keeps newest range            |
      | time past every range              |
      | time stops before first kept       |
      | bytes oldest only                  |
      | bytes exact debt                   |
      | bytes budget too small             |
      | index already fits budget          |
      | delete floor                       |
      | floor past every range             |
      | predicates union                   |
      | expired range still pays size debt |

  Scenario Outline: retention prefix boundaries follow kafkas strict and inclusive comparisons
    Then diskless tests reclaim needs the grace period and no reference: retention prefix boundaries follow kafkas strict and inclusive comparisons for <example>

    Examples:
      | example                    |
      | floor equal to last offset |
      | timestamp equal to horizon |
      | zero diff zero byte range  |
      | horizon below min          |
      | horizon above max          |
      | size sum does not overflow |
      | newest range is preserved  |
      | empty index                |
