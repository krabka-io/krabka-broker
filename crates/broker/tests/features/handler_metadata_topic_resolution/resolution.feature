Feature: Metadata topic identity resolution changes at Kafka API version 12
  Background:
    Given topics resolution-a and resolution-b exist with their own UUIDs and a complete metadata baseline

  Scenario Outline: Names and UUIDs resolve according to the requested Kafka version
    Given Metadata API version <version> carries topic references <rows>
    When the broker answers the versioned topic metadata request
    Then the complete Metadata response contains ordered topic rows <expected>
    Examples:
      | version | rows                                                     | expected                             |
      | 11      | resolution-a/zero                                        | described resolution-a               |
      | 12      | resolution-a/resolution-b                                | described resolution-b               |
      | 12      | resolution-a/zero;null/resolution-b                       | described resolution-b               |
      | 12      | resolution-a/unknown                                     | unknown UUID                         |
      | 13      | null/unknown;null/resolution-a;null/zero                   | unknown UUID;described resolution-a  |
      | 13      | null/resolution-a;null/resolution-a                       | described resolution-a               |
      | 12      | resolution-a/zero;resolution-a/zero                       | described resolution-a               |
      | 12      | no-such-topic/zero                                       | unknown name                         |

  Scenario Outline: Invalid UUID or null-name combinations fail the complete versioned request
    Given Metadata API version <version> carries topic references <rows>
    When the broker answers the versioned topic metadata request
    Then the complete Metadata request error response has code <code>
    Examples:
      | version | rows                               | code |
      | 11      | resolution-a/resolution-a          | 42   |
      | 10      | null/zero                          | 42   |
      | 11      | null/unknown;resolution-a/zero     | 42   |
      | 12      | null/zero;resolution-a/zero        | -1   |
      | 13      | null/zero                          | -1   |
