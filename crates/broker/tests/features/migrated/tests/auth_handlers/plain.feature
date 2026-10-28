Feature: tests auth handlers plain

  Scenario: sasl plain happy path
    Then auth handlers plain: sasl plain happy path

  Scenario: sasl plain authentication metrics tick for success and failure
    Then auth handlers plain: sasl plain authentication metrics tick for success and failure

  Scenario: sasl plain wrong password closes connection
    Then auth handlers plain: sasl plain wrong password closes connection

  Scenario: plain session capped by connections max reauth then closes
    Then auth handlers plain: plain session capped by connections max reauth then closes

  Scenario: plain in band reauth same principal reopens data plane
    Then auth handlers plain: plain in band reauth same principal reopens data plane

  Scenario: plain in band reauth with different principal closes
    Then auth handlers plain: plain in band reauth with different principal closes

  Scenario: plain authenticate without handshake closes the connection
    Then auth handlers plain: plain authenticate without handshake closes the connection

  Scenario: plain reauth after the window elapsed keeps serving round after round
    Then auth handlers plain: plain reauth after the window elapsed keeps serving round after round
