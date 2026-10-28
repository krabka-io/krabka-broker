Feature: Kafka per-IP connection override parsing
  Quoted input preserves leading and trailing whitespace. Parsing follows Java
  trimming and trailing-empty rules, keeps the final duplicate, and rejects malformed
  values as a whole instead of partially applying their valid entries.

  Scenario: Empty text means no per-IP overrides
    Given the per-IP connection override text is ""
    When Kafka per-IP override parsing is applied
    Then the parsed per-IP overrides are:
      | host | maximum_connections |

  Scenario: An IPv4 override names its connection cap
    Given the per-IP connection override text is "127.0.0.1:2"
    When Kafka per-IP override parsing is applied
    Then the parsed per-IP overrides are:
      | host | maximum_connections |
      | 127.0.0.1 | 2 |

  Scenario: Java-style trimming applies around hosts counts and commas
    Given the per-IP connection override text is " a.example : 3 , b:4 "
    When Kafka per-IP override parsing is applied
    Then the parsed per-IP overrides are:
      | host | maximum_connections |
      | a.example | 3 |
      | b | 4 |

  Scenario: IPv6 is split from the count at the final colon
    Given the per-IP connection override text is "::1:5"
    When Kafka per-IP override parsing is applied
    Then the parsed per-IP overrides are:
      | host | maximum_connections |
      | ::1 | 5 |

  Scenario: A trailing comma is discarded like Java split
    Given the per-IP connection override text is "a:1,"
    When Kafka per-IP override parsing is applied
    Then the parsed per-IP overrides are:
      | host | maximum_connections |
      | a | 1 |

  Scenario: The last duplicate host overrides its earlier cap
    Given the per-IP connection override text is "a:1,a:2"
    When Kafka per-IP override parsing is applied
    Then the parsed per-IP overrides are:
      | host | maximum_connections |
      | a | 2 |

  Scenario: A negative cap is clamped to zero
    Given the per-IP connection override text is "a:-1"
    When Kafka per-IP override parsing is applied
    Then the parsed per-IP overrides are:
      | host | maximum_connections |
      | a | 0 |

  Scenario: A leading empty entry rejects the whole configuration
    Given the per-IP connection override text is ",a:1"
    When Kafka per-IP override parsing is applied
    Then the entire per-IP override value is rejected

  Scenario: A missing colon rejects the whole configuration
    Given the per-IP connection override text is "a"
    When Kafka per-IP override parsing is applied
    Then the entire per-IP override value is rejected

  Scenario: A nonnumeric cap rejects the whole configuration
    Given the per-IP connection override text is "a:x"
    When Kafka per-IP override parsing is applied
    Then the entire per-IP override value is rejected

  Scenario: An invalid hostname rejects the whole configuration
    Given the per-IP connection override text is "a/b:1"
    When Kafka per-IP override parsing is applied
    Then the entire per-IP override value is rejected
