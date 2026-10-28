Feature: SCRAM client-first grammar exposes only valid Kafka identities and extensions

  Scenario Outline: Parse a valid SCRAM first message
    Given a SCRAM client sends first message <message>
    When the broker parses the SCRAM client-first identity and extensions
    Then the SCRAM identity has authorization id <authorization_id>, SASL name <name>, and JSON extensions <extensions>

    Examples:
      | message | authorization_id | name | extensions |
      | n,,n=alice,r=abc | unset | alice | [] |
      | n,a=alice,n=alice,r=abc | alice | alice | [] |
      | n,,m=ext,n=alice,r=abc | unset | alice | [] |
      | n,,n=tok,r=abc,tokenauth=true,other=x | unset | tok | [["tokenauth","true"],["other","x"]] |
      | n,,n=a=2Cb=3Dc,r=abc | unset | a=2Cb=3Dc | [] |

  Scenario Outline: Refuse a malformed SCRAM first message
    Given a SCRAM client sends first message <message>
    When the broker parses the SCRAM client-first identity and extensions
    Then the SCRAM client-first message is refused

    Examples:
      | message |
      | y,,n=alice,r=abc |
      | n,,n=,r=abc |
      | n,,n=alice,r= |
      | n,,n=a=41,r=abc |
      | n,,n=alice,r=abc,token_auth=true |
      | n,,r=abc,n=alice |
