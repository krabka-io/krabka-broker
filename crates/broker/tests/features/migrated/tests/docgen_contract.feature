Feature: tests docgen contract

  Scenario: file config schema has the shape docgen renders
    Then docgen contract: file config schema has the shape docgen renders

  Scenario: supported apis is non empty and unique by key
    Then docgen contract: supported apis is non empty and unique by key

  Scenario: topic config docs carry the columns docgen prints
    Then docgen contract: topic config docs carry the columns docgen prints

  Scenario: scenarios record the trace shape docgen renders
    Then docgen contract: scenarios record the trace shape docgen renders
