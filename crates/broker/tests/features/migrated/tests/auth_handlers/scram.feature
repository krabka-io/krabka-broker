Feature: tests auth handlers scram

  Scenario: sasl scram sha512 happy path
    Then auth handlers scram: sasl scram sha512 happy path

  Scenario: sasl scram sha512 wrong password closes connection
    Then auth handlers scram: sasl scram sha512 wrong password closes connection

  Scenario: sasl scram sha256 happy path
    Then auth handlers scram: sasl scram sha256 happy path

  Scenario: sasl scram sha256 wrong password closes connection
    Then auth handlers scram: sasl scram sha256 wrong password closes connection

  Scenario: scram credential below 4096 iterations is refused in round one
    Then auth handlers scram: scram credential below 4096 iterations is refused in round one

  Scenario: a scram proof of the wrong length closes with no response
    Then auth handlers scram: a scram proof of the wrong length closes with no response

  Scenario: scram session capped by connections max reauth then closes
    Then auth handlers scram: scram session capped by connections max reauth then closes

  Scenario: scram in band reauth same principal reopens data plane
    Then auth handlers scram: scram in band reauth same principal reopens data plane

  Scenario: scram in band reauth with different principal closes
    Then auth handlers scram: scram in band reauth with different principal closes

  Scenario: scram reauth after the window elapsed keeps serving round after round
    Then auth handlers scram: scram reauth after the window elapsed keeps serving round after round
