Feature: src config keys validation tests

  Scenario Outline: validate segment bytes floors at kafkas one mebibyte
    Then config keys validation tests: validate segment bytes floors at kafkas one mebibyte for <example>

    Examples:
      | example          |
      | zero             |
      | one              |
      | fourteen         |
      | just below floor |
      | floor            |
      | above floor      |
      | non integer      |

  Scenario Outline: validate retention bytes accepts every long kafka accepts
    Then config keys validation tests: validate retention bytes accepts every long kafka accepts for <example>

    Examples:
      | example       |
      | minus one     |
      | minus two     |
      | zero          |
      | positive      |
      | i64 min       |
      | i64 max       |
      | above i64 max |
      | non integer   |

  Scenario Outline: validate cleanup policy follows kafkas valid list
    Then config keys validation tests: validate cleanup policy follows kafkas valid list for <example>

    Examples:
      | example                    |
      | delete                     |
      | compact                    |
      | compact then delete        |
      | delete then compact        |
      | compact then spaced delete |
      | junk                       |
      | compact then junk          |
      | trailing compact separator |
      | trailing delete separator  |
      | duplicate delete           |
      | duplicate compact          |
      | empty                      |
      | whitespace                 |

  Scenario Outline: validate compression accepts kafkas six names and no others
    Then config keys validation tests: validate compression accepts kafkas six names and no others for <example>

    Examples:
      | example        |
      | producer       |
      | uncompressed   |
      | gzip           |
      | snappy         |
      | lz4            |
      | zstd           |
      | none           |
      | empty          |
      | uppercase gzip |
      | leading space  |

  Scenario Outline: an int key refuses what kafkas int cannot hold
    Then config keys validation tests: an int key refuses what kafkas int cannot hold for <example>

    Examples:
      | example                 |
      | segment bytes max       |
      | segment bytes above max |
      | segment bytes i64 max   |
      | min isr max             |
      | min isr above max       |
      | min isr zero            |
      | min isr negative        |

  Scenario Outline: the remaining kafka topic config names are accepted
    Then config keys validation tests: the remaining kafka topic config names are accepted for <example>

    Examples:
      | example                         |
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

  Scenario: the compaction lag conflict carries kafkas message
    Then config keys validation tests: the compaction lag conflict carries kafkas message

  Scenario: tiered storage rules follow kafkas log config
    Then config keys validation tests: tiered storage rules follow kafkas log config

  Scenario Outline: kafka trunks newest topic keys refuse what kafka refuses
    Then config keys validation tests: kafka trunks newest topic keys refuse what kafka refuses for <example>

    Examples:
      | example                                  |
      | remote copy lag ms                       |
      | remote copy lag bytes                    |
      | max decompressed message bytes zero      |
      | max decompressed message bytes too large |
      | errors deadletterqueue group enable      |

  Scenario Outline: kafka trunks newest topic keys need unstable api versions
    Then config keys validation tests: kafka trunks newest topic keys need unstable api versions for <example>

    Examples:
      | example                             |
      | remote copy lag ms                  |
      | remote copy lag bytes               |
      | max decompressed message bytes      |
      | errors deadletterqueue group enable |

  Scenario: trunk topic keys take no cluster default under kafka 4 3 1
    Then config keys validation tests: trunk topic keys take no cluster default under kafka 4 3 1

  Scenario Outline: values are parsed the way kafkas config def parses them
    Then config keys validation tests: values are parsed the way kafkas config def parses them for <example>

    Examples:
      | example                  |
      | uppercase boolean        |
      | whitespace boolean       |
      | invalid boolean          |
      | trimmed long             |
      | trimmed int              |
      | trimmed enum             |
      | double suffix            |
      | integral double          |
      | spaced cleanup list      |
      | empty cleanup list       |
      | blank leader throttle    |
      | wildcard leader throttle |
      | leader throttle list     |
      | negative leader throttle |
      | signed leader throttle   |
      | spaced follower throttle |

  Scenario Outline: refusals carry kafkas config exception text
    Then config keys validation tests: refusals carry kafkas config exception text for <example>

    Examples:
      | example                    |
      | unknown key                |
      | negative max message bytes |
      | lz4 level too high         |
      | gzip level zero            |
      | non integer retention ms   |
      | segment bytes int overflow |
      | invalid boolean            |
      | invalid compression type   |
      | invalid timestamp type     |
      | invalid cleanup policy     |
      | duplicate cleanup policy   |
      | empty cleanup policy item  |
      | dirty ratio too high       |
      | invalid follower throttle  |
