Feature: Kafka feature negotiation compatibility
  Older ApiVersions clients cannot consume zero-minimum feature ranges.
  A new metadata image finalizes only Kafka's minimum metadata version.

  Scenario Outline: Legacy clients see only nonzero minimum feature ranges
    Given the client uses ApiVersions version <version>
    When the broker advertises production supported feature ranges
    Then the complete supported feature ranges are:
      | feature          | minimum          | maximum             |
      | metadata.version | minimum-metadata | production-metadata |

    Examples:
      | version |
      | 0       |
      | 3       |

  Scenario Outline: Modern clients see the complete production feature catalog
    Given the client uses ApiVersions version <version>
    When the broker advertises production supported feature ranges
    Then the complete supported feature ranges are:
      | feature                         | minimum          | maximum             |
      | metadata.version                | minimum-metadata | production-metadata |
      | group.version                   | 0                | 1                   |
      | transaction.version             | 0                | 2                   |
      | share.version                   | 0                | 1                   |
      | streams.version                 | 0                | 1                   |
      | eligible.leader.replicas.version | 0                | 1                   |
      | kraft.version                   | 0                | 1                   |
      | krabka.version                  | 0                | 2                   |

    Examples:
      | version |
      | 4       |
      | 5       |

  Scenario: A fresh image finalizes the minimum metadata version without zero-level features
    Given the broker has a fresh metadata image
    When the broker advertises the finalized feature levels
    Then the complete finalized feature levels are:
      | feature          | level            |
      | metadata.version | minimum-metadata |
