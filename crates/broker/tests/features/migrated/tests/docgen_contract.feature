Feature: Public reference contracts consumed by docgen

  Scenario: file config schema has the shape docgen renders
    Given docgen reads the configuration schema reference source
    When docgen obtains the selected reference data through its public entry point
    Then file config schema has the shape docgen renders

  Scenario: supported apis is non empty and unique by key
    Given docgen reads the client API catalog reference source
    When docgen obtains the selected reference data through its public entry point
    Then supported apis is non empty and unique by key

  Scenario: topic config docs carry the columns docgen prints
    Given docgen reads the topic configuration registry reference source
    When docgen obtains the selected reference data through its public entry point
    Then topic config docs carry the columns docgen prints

  Scenario: scenarios record the trace shape docgen renders
    Given docgen reads the Raft simulation catalog reference source
    When docgen obtains the selected reference data through its public entry point
    Then scenarios record the trace shape docgen renders
