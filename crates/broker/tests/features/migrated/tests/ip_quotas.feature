Feature: tests ip quotas

  Scenario: ip quota alter then describe round trip
    Then ip quotas: ip quota alter then describe round trip

  Scenario: connection creation rate closes the throttled connection
    Then ip quotas: connection creation rate closes the throttled connection

  Scenario: connection creation rate matches another spelling of the address
    Then ip quotas: connection creation rate matches another spelling of the address

  Scenario: unthrottled ip unaffected
    Then ip quotas: unthrottled ip unaffected

  Scenario: max connections per ip refuses excess and frees on close
    Then ip quotas: max connections per ip refuses excess and frees on close
