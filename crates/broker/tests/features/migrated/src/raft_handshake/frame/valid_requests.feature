Feature: Controller handshake reads flexible and legacy Kafka request headers

  Scenario Outline: Read a valid Kafka header and payload
    Given the controller peer sent Kafka frame hex <frame>
    When the controller handshake reads the Kafka request frame
    Then the request has API <key> version <version>, correlation <correlation>, and body <body>

    Examples:
      | frame | key | version | correlation | body |
      # nonflexible header without client
      | 00000014001100010000002affff706c61696e2d626f6479 | 17 | 1 | 42 | plain-body |
      # flexible authenticate header
      | 00000015002400020000002b00016300617574682d626f6479 | 36 | 2 | 43 | auth-body |
      # flexible API versions header skips unknown tag300
      | 00000022001200030000002c00016301ac020b7461672d7061796c6f61646170692d626f6479 | 18 | 3 | 44 | api-body |
      # client ends exactly at frame boundary
      | 00000010001100010000002d0006636c69656e74 | 17 | 1 | 45 | empty |
      # null client and exact ten-byte header
      | 0000000a001100010000002effff | 17 | 1 | 46 | empty |
