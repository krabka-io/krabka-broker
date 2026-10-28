Feature: tests soak contract alerts

  Scenario Outline: tests: a target port comes from the metrics url
    Then soak contract alerts tests: a target port comes from the metrics url for <example>

    Examples:
      | example  |
      | loopback |
      | no path  |
      | no port  |

  Scenario: tests: the config scrapes every node as the job the rules name
    Then soak contract alerts tests: the config scrapes every node as the job the rules name

  Scenario Outline: tests: the api port is the published one
    Then soak contract alerts tests: the api port is the published one for <example>

    Examples:
      | example       |
      | ipv4          |
      | both families |
      | empty         |

  Scenario Outline: tests: ready is a 200
    Then soak contract alerts tests: ready is a 200 for <example>

    Examples:
      | example         |
      | ready           |
      | starting        |
      | closed by proxy |

  Scenario Outline: tests: milliseconds go out as seconds
    Then soak contract alerts tests: milliseconds go out as seconds for <example>

    Examples:
      | example |
      | whole   |
      | millis  |

  Scenario Outline: tests: seconds come back as milliseconds
    Then soak contract alerts tests: seconds come back as milliseconds for <example>

    Examples:
      | example     |
      | integer     |
      | one digit   |
      | past millis |
      | exponent    |

  Scenario Outline: tests: a long run is queried in ranges prometheus accepts
    Then soak contract alerts tests: a long run is queried in ranges prometheus accepts for <example>

    Examples:
      | example                  |
      | one range                |
      | a single point           |
      | split at the point limit |

  Scenario Outline: tests: alerts fold into intervals per state
    Then soak contract alerts tests: alerts fold into intervals per state for <example>

    Examples:
      | example                           |
      | pending then firing then resolved |
      | still firing at the last step     |
      | a gap is two intervals            |

  Scenario: tests: ranges of one series join into one interval
    Then soak contract alerts tests: ranges of one series join into one interval

  Scenario Outline: tests: a bad answer is an error
    Then soak contract alerts tests: a bad answer is an error for <example>

    Examples:
      | example       |
      | an error      |
      | a vector      |
      | not an answer |

  Scenario: tests: only alerting rules are recorded with their health
    Then soak contract alerts tests: only alerting rules are recorded with their health

  Scenario: tests: the lines name unhealthy rules and alerts that fired
    Then soak contract alerts tests: the lines name unhealthy rules and alerts that fired

  Scenario Outline: tests: a span reads in hours minutes and seconds
    Then soak contract alerts tests: a span reads in hours minutes and seconds for <example>

    Examples:
      | example |
      | seconds |
      | minutes |
      | hours   |
