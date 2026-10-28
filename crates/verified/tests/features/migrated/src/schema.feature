Feature: Per-field schema validation selection
  Key and value validation are configured independently. Null fields, including
  tombstone values, are exempt even when their field role is enabled.

  Scenario: Only configured nonnull fields require schema validation (1: boundary key validation, keys true)
    Given schema validation enabled for keys true and values false
    When a key field with a nonnull payload is inspected
    Then schema validation will check-key

  Scenario: Only configured nonnull fields require schema validation (2: boundary value validation, keys false)
    Given schema validation enabled for keys false and values true
    When a value field with a nonnull payload is inspected
    Then schema validation will check-value

  Scenario: Only configured nonnull fields require schema validation (3: boundary both enabled for key, keys true)
    Given schema validation enabled for keys true and values true
    When a key field with a nonnull payload is inspected
    Then schema validation will check-key

  Scenario: Only configured nonnull fields require schema validation (4: boundary both enabled for value, keys true)
    Given schema validation enabled for keys true and values true
    When a value field with a nonnull payload is inspected
    Then schema validation will check-value

  Scenario: Only configured nonnull fields require schema validation (5: boundary null key, keys true)
    Given schema validation enabled for keys true and values true
    When a key field with a null payload is inspected
    Then schema validation will skip-field

  Scenario: Only configured nonnull fields require schema validation (6: boundary tombstone value, keys true)
    Given schema validation enabled for keys true and values true
    When a value field with a null payload is inspected
    Then schema validation will skip-field

  Scenario: Only configured nonnull fields require schema validation (7: boundary only values are enabled, keys false)
    Given schema validation enabled for keys false and values true
    When a key field with a nonnull payload is inspected
    Then schema validation will skip-field

  Scenario: Only configured nonnull fields require schema validation (8: boundary only keys are enabled, keys true)
    Given schema validation enabled for keys true and values false
    When a value field with a nonnull payload is inspected
    Then schema validation will skip-field

  Scenario: Only configured nonnull fields require schema validation (9: boundary validation disabled, keys false)
    Given schema validation enabled for keys false and values false
    When a value field with a nonnull payload is inspected
    Then schema validation will skip-field
