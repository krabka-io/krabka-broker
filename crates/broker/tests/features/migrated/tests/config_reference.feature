Feature: Operator configuration reference

  Scenario: checked in schema matches file config
    Given the checked-in configuration schema and reference page
    When the FileConfig schema is generated
    Then the generated schema matches the checked-in schema

  Scenario: reference page carries every schema key
    Given the checked-in configuration schema and reference page
    When the FileConfig schema is generated
    Then the reference page has a generated banner and a row for every schema key

  Scenario: print config schema flag prints the schema
    Given the broker executable and its library configuration schema
    When the broker is invoked with --print-config-schema
    Then the command succeeds and prints the library schema as JSON

  Scenario: schema names the top level sections
    Given the checked-in configuration schema and reference page
    When the FileConfig schema is generated
    Then the schema exposes every operator configuration section

  Scenario: unit bearing fields carry their format
    Given the checked-in configuration schema and reference page
    When the FileConfig schema is generated
    Then duration and byte-size fields advertise their units
