Feature: src restore tests archive reconciliation covers every scan and snapshot pair

  Scenario Outline: record coordinates follow kafka timestamp type
    Then restore tests archive reconciliation covers every scan and snapshot pair: record coordinates follow kafka timestamp type for <example>

    Examples:
      | example                           |
      | create time adds delta            |
      | log append uses batch max         |
      | log append ignores delta overflow |
      | create time overflow above        |
      | create time overflow below        |
      | create time reaches min           |
      | negative delta                    |
      | delta past span                   |
      | negative span                     |
      | offset overflow                   |
      | extreme representable             |

  Scenario Outline: rewritten headers admit exactly kafka producer and control layouts
    Then restore tests archive reconciliation covers every scan and snapshot pair: rewritten headers admit exactly kafka producer and control layouts for <example>

    Examples:
      | example                               |
      | non idempotent                        |
      | idempotent empty                      |
      | transactional data                    |
      | transaction marker                    |
      | compacted marker                      |
      | barrier control                       |
      | control spans offsets                 |
      | control has two records               |
      | marker has sequence                   |
      | transactional marker without producer |
      | transactional data without producer   |
      | partial sentinel epoch                |
      | producer without epoch                |
      | negative span                         |
      | negative count                        |
      | negative base                         |
      | frontier overflow                     |
