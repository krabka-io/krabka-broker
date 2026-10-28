Feature: src config keys broker dynamic

  Scenario Outline: tests: broker resource names follow kafkas node id rule
    Then config keys broker dynamic tests: broker resource names follow kafkas node id rule for <example>

    Examples:
      | example          |
      | default resource |
      | serving node     |
      | other node       |
      | negative node    |
      | non integer      |

  Scenario Outline: tests: dynamic broker configs follow kafkas validate configs
    Then config keys broker dynamic tests: dynamic broker configs follow kafkas validate configs for <example>

    Examples:
      | example                                                 |
      | custom key allowed                                      |
      | broker log dirs refused                                 |
      | ssl key requires listener prefix                        |
      | listener ssl key requires named broker                  |
      | listener ssl key allowed on named broker                |
      | topic retention value allowed                           |
      | dynamic integer value allowed                           |
      | throttle rate must be nonnegative                       |
      | minimum insync replicas must be positive                |
      | startup only key refused                                |
      | queued max requests refused                             |
      | non dynamic key precedes bad value                      |
      | offsets replication factor refused                      |
      | dynamic integer value must parse                        |
      | connection limit must be nonnegative                    |
      | network threads must be positive                        |
      | tiered offset boolean is case insensitive               |
      | listener override reports base key                      |
      | per broker key refused on cluster default               |
      | per broker key allowed on named broker                  |
      | consumer assignment interval at the maximum             |
      | consumer assignment interval at the minimum             |
      | consumer assignment interval above the maximum          |
      | share assignment interval above the maximum             |
      | streams assignment interval above the maximum           |
      | consumer interval is checked before share               |
      | negative assignment interval fails the type check first |
