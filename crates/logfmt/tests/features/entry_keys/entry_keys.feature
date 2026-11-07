Feature: Entries carry their time and origin under keys the formatter owns

  The formatter writes timestamp, severity and target on every entry. Cloud
  Logging takes the entry timestamp from timestamp, and an operator finds the
  code that logged through target. Severities are described in
  severity/severity.feature.

  Rule: The timestamp is the time of writing, in RFC 3339 UTC

    Scenario: A connection event is stamped with the time of writing
      When the broker logs that a connection opened
      Then the entry timestamp is the time of writing in RFC 3339 UTC

  Rule: The target names the code that logged

    Scenario: The target is the module path of the logging code
      When the broker logs an event at level INFO
      Then the entry target is the module path of the logging code

    Scenario: An explicit target replaces the module path
      When the broker logs an authorizer decision
      Then the entry target reads kafka.authorizer.logger

  Rule: An event field named like an entry key leaves that key intact

    Scenario: A severity field does not change the severity
      When the broker logs a warning with a severity field of DEBUG
      Then the entry severity is WARNING

    Scenario: A target field does not change the target
      When the broker logs a warning with a target field of forged.module
      Then the entry target is the module path of the logging code

    Scenario: A timestamp field does not change the timestamp
      When the broker logs a warning with a timestamp field of 1970-01-01T00:00:00Z
      Then the entry timestamp is the time of writing in RFC 3339 UTC
