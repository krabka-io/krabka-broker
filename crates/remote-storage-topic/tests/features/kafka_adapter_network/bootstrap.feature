Feature: Kafka metadata adapter startup
  Scenario Outline: Invalid policy wins over invalid bootstrap
    Given the metadata log has a zero creation timeout and an invalid bootstrap address
    When the metadata log is opened in <mode> mode
    Then the creation timeout is rejected before bootstrap resolution
    Examples:
      | mode       |
      | read only  |
      | read write |

  Scenario Outline: Bootstrap resolution identifies unusable addresses
    Given the bootstrap address text is <bootstrap>
    When bootstrap addresses are resolved
    Then bootstrap resolution reports <message>
    Examples:
      | bootstrap | message                      |
      | no-port   | DNS lookup of no-port failed |

  Scenario: Empty bootstrap has no resolved addresses
    Given the bootstrap address text is empty
    When bootstrap addresses are resolved
    Then bootstrap resolution reports bootstrap "" resolved to no address

  Scenario: Separators do not constitute a bootstrap address
    Given the bootstrap address text contains only separators
    When bootstrap addresses are resolved
    Then bootstrap resolution reports bootstrap " , " resolved to no address

  Scenario: Resolution preserves address order and ignores empty entries
    Given the bootstrap address text is 127.0.0.1:9092, ,localhost:9093
    When bootstrap addresses are resolved
    Then the numeric address precedes all localhost addresses on port 9093
