Feature: Kafka-compatible formatter planning
  Scenario Outline: Feature validation reports the first error in Kafka order
    Given format flags <flags>
    When the formatter plans the run
    Then the first formatter failure has Kafka feature exit code and begins <message>

    Examples:
      | flags | message |
      | --release-version 9.9-IV0 --feature kraft.version=9 | krabka format: Unknown metadata.version '9.9-IV0'. |
      | --feature nope.version=1 --feature kraft.version=9 | krabka format: Unsupported feature: nope.version. |
      | --feature kraft.version=9 | krabka format: kraft.version=9 requires --standalone, |
      | --no-initial-controllers --feature kraft.version=9 | krabka format: No feature:kraft.version with feature level 9 |

  Scenario: The first log directory is metadata by default
    Given log directories a,b with metadata directory first-log-dir in static mode
    When the formatter plans the run
    Then the ordered format targets and directory kinds are
      | directory | kind |
      | a | static-metadata |
      | b | data |

  Scenario: A separate metadata directory leads the targets
    Given log directories a,b with metadata directory m in static mode
    When the formatter plans the run
    Then the ordered format targets and directory kinds are
      | directory | kind |
      | m | static-metadata |
      | a | data |
      | b | data |

  Scenario: A metadata directory also listed as a log directory is deduplicated
    Given log directories a,b with metadata directory b in static mode
    When the formatter plans the run
    Then the ordered format targets and directory kinds are
      | directory | kind |
      | b | static-metadata |
      | a | data |

  Scenario: One directory can hold both metadata and data
    Given log directories a with metadata directory a in static mode
    When the formatter plans the run
    Then the ordered format targets and directory kinds are
      | directory | kind |
      | a | static-metadata |

  Scenario: Repeated log directory flags are deduplicated
    Given log directories a,a with metadata directory m in static mode
    When the formatter plans the run
    Then the ordered format targets and directory kinds are
      | directory | kind |
      | m | static-metadata |
      | a | data |

  Scenario: A standalone controller formats voter metadata
    Given log directories a with metadata directory m in standalone mode
    When the formatter plans the run
    Then the ordered format targets and directory kinds are
      | directory | kind |
      | m | dynamic-voter |
      | a | data |

  Scenario: A nonvoting dynamic controller formats dynamic metadata
    Given log directories a with metadata directory m in nonvoter mode
    When the formatter plans the run
    Then the ordered format targets and directory kinds are
      | directory | kind |
      | m | dynamic-metadata |
      | a | data |

  Scenario: Unreadable default metadata refuses the format
    Given log directories a,b with metadata directory first-log-dir in static mode
    And directory a contains unreadable meta.properties
    When the formatter plans the run
    Then formatting refuses unreadable metadata directory a

  Scenario: Unreadable separate metadata refuses the format
    Given log directories a with metadata directory m in static mode
    And directory m contains unreadable meta.properties
    When the formatter plans the run
    Then formatting refuses unreadable metadata directory m

  Scenario: An unreadable data directory is reported while usable targets remain
    Given log directories a,b with metadata directory m in static mode
    And directory a contains unreadable meta.properties
    When the formatter plans the run
    Then the formatter keeps target directories m,b and reports unreadable directories a
