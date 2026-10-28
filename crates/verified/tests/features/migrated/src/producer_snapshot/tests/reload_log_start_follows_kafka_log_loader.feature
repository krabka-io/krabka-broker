Feature: src producer snapshot tests reload log start follows kafka log loader

  Scenario Outline: reload log start follows kafka log loader
    Then producer snapshot tests reload log start follows kafka log loader: reload log start follows kafka log loader for <example>

    Examples:
      | example                                 |
      | remote checkpoint below local           |
      | remote without checkpoint               |
      | local uses segment base over checkpoint |
      | local without checkpoint                |

  Scenario Outline: stray snapshots follow kafka remove stray snapshots
    Then producer snapshot tests reload log start follows kafka log loader: stray snapshots follow kafka remove stray snapshots for <example>

    Examples:
      | example                           |
      | all match segment bases           |
      | older strays removed              |
      | clean shutdown snapshot preserved |
      | only newest above bases preserved |
      | newest snapshot without segments  |
      | input order is irrelevant         |

  Scenario Outline: reload keeps exactly the half open range above the log start
    Then producer snapshot tests reload log start follows kafka log loader: reload keeps exactly the half open range above the log start for <example>

    Examples:
      | example          |
      | below log start  |
      | at log start     |
      | inside log range |
      | at log end       |
      | above log end    |
      | negative offset  |

  Scenario Outline: latest snapshot is the newest one the reload keeps
    Then producer snapshot tests reload log start follows kafka log loader: latest snapshot is the newest one the reload keeps for <example>

    Examples:
      | example                      |
      | no snapshots                 |
      | all offsets out of range     |
      | newest kept in unsorted list |
      | start boundary is excluded   |
      | duplicate latest keeps first |
      | maximum offset is valid      |

  Scenario Outline: entry validation is exact and snapshot bounded
    Then producer snapshot tests reload log start follows kafka log loader: entry validation is exact and snapshot bounded for <example>

    Examples:
      | example                          |
      | real producer                    |
      | marker only producer             |
      | integer extremes                 |
      | negative snapshot offset         |
      | negative producer id             |
      | negative producer epoch          |
      | coordinator epoch below sentinel |
      | last record at boundary          |
      | delta below zero                 |
      | sentinel with delta              |
      | transaction at boundary          |
      | transaction after last record    |
      | transaction below sentinel       |
