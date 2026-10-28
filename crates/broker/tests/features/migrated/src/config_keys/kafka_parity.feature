Feature: src config keys kafka parity

  Scenario: every kafka 4 3 1 topic config is accepted by the alter path
    Then config keys kafka parity: every kafka 4 3 1 topic config is accepted by the alter path

  Scenario: kafka trunk topic configs are accepted only under unstable api versions
    Then config keys kafka parity: kafka trunk topic configs are accepted only under unstable api versions

  Scenario: a whole kafka config set replays in one alter
    Then config keys kafka parity: a whole kafka config set replays in one alter

  Scenario: kafka topic key types and defaults match
    Then config keys kafka parity: kafka topic key types and defaults match

  Scenario: every kafka topic key is documented
    Then config keys kafka parity: every kafka topic key is documented

  Scenario Outline: kafka refuses what krabka refuses
    Then config keys kafka parity: kafka refuses what krabka refuses for <example>

    Examples:
      | example                       |
      | message downconversion enable |
      | made up key                   |
      | gzip level zero               |
      | lz4 level too high            |
      | lz4 level zero                |
      | zstd level too low            |
      | zstd level too high           |
      | negative max message bytes    |
      | segment bytes below floor     |
      | segment bytes zero            |
      | compression none              |

  Scenario Outline: a key kafka gives no validator accepts every value of its type
    Then config keys kafka parity: a key kafka gives no validator accepts every value of its type for <example>

    Examples:
      | example   |
      | i64 min   |
      | minus two |
      | minus one |
      | zero      |
      | i64 max   |

  Scenario: retention bytes refuses a value kafka long cannot hold
    Then config keys kafka parity: retention bytes refuses a value kafka long cannot hold

  Scenario Outline: codec levels accept the bounds kafka accepts
    Then config keys kafka parity: codec levels accept the bounds kafka accepts for <example>

    Examples:
      | example        |
      | gzip unlimited |
      | gzip minimum   |
      | gzip maximum   |
      | lz4 minimum    |
      | lz4 maximum    |
      | zstd minimum   |
      | zstd maximum   |

  Scenario: the topic key sets differ only by the rosters below
    Then config keys kafka parity: the topic key sets differ only by the rosters below

  Scenario: every recorded divergence is still a divergence
    Then config keys kafka parity: every recorded divergence is still a divergence
