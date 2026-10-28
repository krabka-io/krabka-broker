Feature: src config keys registry tests

  Scenario: config type wire bytes match kafka
    Then config keys registry tests: config type wire bytes match kafka

  Scenario Outline: topic key types match apache kafka
    Then config keys registry tests: topic key types match apache kafka for <example>

    Examples:
      | example                         |
      | retention ms                    |
      | retention bytes                 |
      | segment bytes                   |
      | cleanup policy                  |
      | compression type                |
      | min insync replicas             |
      | unclean leader election enable  |
      | remote storage enable           |
      | local retention ms              |
      | local retention bytes           |
      | delete retention ms             |
      | leader throttled replicas       |
      | follower throttled replicas     |
      | segment ms                      |
      | segment index bytes             |
      | segment jitter ms               |
      | min compaction lag ms           |
      | max compaction lag ms           |
      | min cleanable dirty ratio       |
      | file delete delay ms            |
      | flush messages                  |
      | flush ms                        |
      | index interval bytes            |
      | preallocate                     |
      | message timestamp type          |
      | message timestamp after max ms  |
      | message timestamp before max ms |

  Scenario Outline: broker key types match apache kafka
    Then config keys registry tests: broker key types match apache kafka for <example>

    Examples:
      | example                                        |
      | leader throttled rate                          |
      | follower throttled rate                        |
      | alter log dirs throttled rate                  |
      | unclean leader election enable                 |
      | remote list offsets request timeout ms         |
      | offsets retention minutes                      |
      | offsets retention check interval ms            |
      | connections max idle ms                        |
      | connections max reauth ms                      |
      | node id                                        |
      | transactional id expiration ms                 |
      | transaction remove expired cleanup interval ms |

  Scenario Outline: topic key defaults match apache kafka
    Then config keys registry tests: topic key defaults match apache kafka for <example>

    Examples:
      | example                         |
      | retention ms                    |
      | retention bytes                 |
      | segment bytes                   |
      | cleanup policy                  |
      | compression type                |
      | min insync replicas             |
      | unclean leader election enable  |
      | remote storage enable           |
      | local retention ms              |
      | local retention bytes           |
      | delete retention ms             |
      | leader throttled replicas       |
      | follower throttled replicas     |
      | segment ms                      |
      | segment index bytes             |
      | segment jitter ms               |
      | min compaction lag ms           |
      | max compaction lag ms           |
      | min cleanable dirty ratio       |
      | file delete delay ms            |
      | flush messages                  |
      | flush ms                        |
      | index interval bytes            |
      | preallocate                     |
      | message timestamp type          |
      | message timestamp after max ms  |
      | message timestamp before max ms |

  Scenario Outline: transactional id expiry defaults match apache kafka
    Then config keys registry tests: transactional id expiry defaults match apache kafka for <example>

    Examples:
      | example                                     |
      | transactional id expiration                 |
      | transaction remove expired cleanup interval |
