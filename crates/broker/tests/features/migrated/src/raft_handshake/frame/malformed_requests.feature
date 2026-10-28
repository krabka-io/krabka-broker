Feature: Controller handshake rejects malformed Kafka request headers

  Scenario Outline: Refuse a malformed Kafka request header
    Given the controller peer sent Kafka frame hex <frame>
    When the controller handshake reads the Kafka request frame
    Then the controller handshake rejects the malformed request as a protocol error

    Examples:
      | case | frame |
      | header payload has 0 bytes | 00000000 |
      | header payload has 1 bytes | 0000000100 |
      | header payload has 2 bytes | 000000020000 |
      | header payload has 3 bytes | 00000003000000 |
      | short nine-byte header | 00000009000000000000000000 |
      | truncated client id | 0000000c001100010000000700037879 |
      | missing flexible tagged-field count | 0000000b002400020000002c000163 |
