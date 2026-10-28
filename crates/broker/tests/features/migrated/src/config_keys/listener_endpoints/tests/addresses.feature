Feature: Listener endpoints follow Kafka address and port rules

  Scenario: wildcard
    Given listener addresses are JSON "PLAINTEXT://:9092"
    When broker listener addresses are resolved against the configured protocols
    Then the resolved listener endpoints are:
      | name | protocol | host | port |
      | PLAINTEXT | Plaintext | wildcard | 9092 |

  Scenario: lower case name
    Given listener addresses are JSON "internal://127.0.0.1:9094"
    When broker listener addresses are resolved against the configured protocols
    Then the resolved listener endpoints are:
      | name | protocol | host | port |
      | INTERNAL | SaslPlaintext | 127.0.0.1 | 9094 |

  Scenario: bracketed ipv6
    Given listener addresses are JSON "SSL://[::1]:9093"
    When broker listener addresses are resolved against the configured protocols
    Then the resolved listener endpoints are:
      | name | protocol | host | port |
      | SSL | Ssl | ::1 | 9093 |

  Scenario: two
    Given listener addresses are JSON "PLAINTEXT://localhost:9092,SSL://localhost:9093"
    When broker listener addresses are resolved against the configured protocols
    Then the resolved listener endpoints are:
      | name | protocol | host | port |
      | PLAINTEXT | Plaintext | localhost | 9092 |
      | SSL | Ssl | localhost | 9093 |

  Scenario: unparsable
    Given listener addresses are JSON "PLAINTEXT:9092"
    When broker listener addresses are resolved against the configured protocols
    Then listener resolution fails with Error creating broker listeners from '[PLAINTEXT:9092]': Unable to parse PLAINTEXT:9092 to a broker endpoint

  Scenario: port overflow
    Given listener addresses are JSON "PLAINTEXT://:99999999999"
    When broker listener addresses are resolved against the configured protocols
    Then listener resolution fails with Error creating broker listeners from '[PLAINTEXT://:99999999999]': For input string: "99999999999"

  Scenario: unmapped
    Given listener addresses are JSON "EXTERNAL://:9095"
    When broker listener addresses are resolved against the configured protocols
    Then listener resolution fails with Error creating broker listeners from '[EXTERNAL://:9095]': No security protocol defined for listener EXTERNAL

  Scenario: repeated name
    Given listener addresses are JSON "PLAINTEXT://:9092,plaintext://:9093"
    When broker listener addresses are resolved against the configured protocols
    Then listener resolution fails with Error creating broker listeners from '[PLAINTEXT://:9092, plaintext://:9093]': Each listener must have a different name, listeners: [PLAINTEXT://:9092, plaintext://:9093]

  Scenario: repeated port
    Given listener addresses are JSON "PLAINTEXT://:9092,SSL://:9092"
    When broker listener addresses are resolved against the configured protocols
    Then listener resolution fails with Error creating broker listeners from '[PLAINTEXT://:9092, SSL://:9092]': Each listener must have a different port, listeners: [PLAINTEXT://:9092, SSL://:9092]

  Scenario: port zero may repeat
    Given listener addresses are JSON "PLAINTEXT://:0,SSL://:0"
    When broker listener addresses are resolved against the configured protocols
    Then the resolved listener endpoints are:
      | name | protocol | host | port |
      | PLAINTEXT | Plaintext | wildcard | 0 |
      | SSL | Ssl | wildcard | 0 |

  Scenario: ipv4 and ipv6 share a port
    Given listener addresses are JSON "PLAINTEXT://127.0.0.1:9092,SSL://[::1]:9092"
    When broker listener addresses are resolved against the configured protocols
    Then the resolved listener endpoints are:
      | name | protocol | host | port |
      | PLAINTEXT | Plaintext | 127.0.0.1 | 9092 |
      | SSL | Ssl | ::1 | 9092 |

  Scenario: two ipv4 share a port
    Given listener addresses are JSON "PLAINTEXT://127.0.0.1:9092,SSL://127.0.0.2:9092"
    When broker listener addresses are resolved against the configured protocols
    Then listener resolution fails with Error creating broker listeners from '[PLAINTEXT://127.0.0.1:9092, SSL://127.0.0.2:9092]': If you have two listeners on the same port then one needs to be IPv4 and the other IPv6, listeners: [PLAINTEXT://127.0.0.1:9092, SSL://127.0.0.2:9092], port: 9092

  Scenario: three share a port
    Given listener addresses are JSON "PLAINTEXT://127.0.0.1:9092,SSL://[::1]:9092,INTERNAL://127.0.0.2:9092"
    When broker listener addresses are resolved against the configured protocols
    Then listener resolution fails with Error creating broker listeners from '[PLAINTEXT://127.0.0.1:9092, SSL://[::1]:9092, INTERNAL://127.0.0.2:9092]': Each listener must have a different port unless exactly one listener has an IPv4 address and the other IPv6 address, listeners: [PLAINTEXT://127.0.0.1:9092, SSL://[::1]:9092, INTERNAL://127.0.0.2:9092], port: 9092
