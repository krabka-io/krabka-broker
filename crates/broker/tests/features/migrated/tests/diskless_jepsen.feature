Feature: tests diskless jepsen

  Scenario: three broker fault schedule preserves the acked ledger
    Then diskless jepsen: three broker fault schedule preserves the acked ledger
