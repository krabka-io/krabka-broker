Feature: tests auth handlers handshake

  Scenario: api versions reachable pre auth on sasl listener
    Then auth handlers handshake: api versions reachable pre auth on sasl listener

  Scenario: metadata rejected pre auth on sasl listener
    Then auth handlers handshake: metadata rejected pre auth on sasl listener

  Scenario: unsupported mechanism answers 33 then closes
    Then auth handlers handshake: unsupported mechanism answers 33 then closes

  Scenario: v0 handshake runs the exchange as raw tokens
    Then auth handlers handshake: v0 handshake runs the exchange as raw tokens

  Scenario: v0 raw token with bad credentials closes without response
    Then auth handlers handshake: v0 raw token with bad credentials closes without response

  Scenario: api versions after the handshake answers 34 then closes
    Then auth handlers handshake: api versions after the handshake answers 34 then closes

  Scenario: a request after the handshake answers illegal sasl state when kafka can parse it
    Then auth handlers handshake: a request after the handshake answers illegal sasl state when kafka can parse it
