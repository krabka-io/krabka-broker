Feature: Entries carry their time and origin under keys the formatter owns

  The formatter writes timestamp, severity and target on every entry. Cloud
  Logging takes the entry timestamp from timestamp, and an operator finds the
  code that logged through target. An event field the formatter cannot write
  at the top level is kept in the fields object instead, where an operator
  filters on it as jsonPayload.fields.severity.

  Depends on: Log levels become Cloud Logging severities - ../severity/severity.feature
  Depends on: Entries link to their trace in Cloud Trace - ../trace_correlation/trace_correlation.feature

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

  Rule: An event field named like an entry key is kept under fields

    Scenario Outline: A field named like an entry key keeps its value under fields
      When the broker logs a warning with a <key> field of <value>
      Then the entry field fields.<key> reads <value>

      Examples:
        | case                                     | key       | value                |
        | a severity of its own                    | severity  | DEBUG                |
        | a target of its own                      | target    | forged.module        |
        | a timestamp of its own                   | timestamp | 1970-01-01T00:00:00Z |
        | a field named fields keeps its own value | fields    | replicas             |

  Rule: An event field named like a Cloud Logging special key is kept under fields, so an event cannot set it

    Cloud Logging lifts these keys out of jsonPayload into the LogEntry itself.

    Scenario Outline: A field named like a special key does not reach the entry
      When the broker logs an event with a field named <key> of forged
      Then the entry has no <key> field
      And the entry field fields.<key> reads forged

      Examples:
        | case                              | key                           |
        | the HTTP request of the entry     | httpRequest                   |
        | the time Cloud Logging stamps     | time                          |
        | the seconds of a split timestamp  | timestampSeconds              |
        | the trace the entry links to      | logging.googleapis.com/trace  |
        | any key under the reserved prefix | logging.googleapis.com/labels |
