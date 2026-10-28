Feature: src raft handshake frame

  Scenario: tests: read kafka request decodes nonflex and flexible headers
    Then raft handshake frame tests: read kafka request decodes nonflex and flexible headers

  Scenario: tests: read kafka request rejects short and truncated headers
    Then raft handshake frame tests: read kafka request rejects short and truncated headers

  Scenario Outline: tests: read kafka request refuses a frame over the sasl receive limit
    Then raft handshake frame tests: read kafka request refuses a frame over the sasl receive limit for <example>

    Examples:
      | example               |
      | exact limit           |
      | one over limit        |
      | maximum u32           |
      | negative kafka length |

  Scenario: tests: read kafka request accepts exact client id end boundary
    Then raft handshake frame tests: read kafka request accepts exact client id end boundary

  Scenario: tests: read kafka request accepts exact header prefix frame
    Then raft handshake frame tests: read kafka request accepts exact header prefix frame
