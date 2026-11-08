Feature: Log levels become Cloud Logging severities

  Cloud Logging reads the severity field of a structured entry and sets the
  entry's LogSeverity from it. LogSeverity has no WARN and nothing finer than
  DEBUG, so two tracing levels change their name on the way out.

  Rule: A level that Cloud Logging also names keeps its name

    Scenario Outline: A level Cloud Logging knows is written as it is
      When the broker logs an event at level <level>
      Then the entry severity is <severity>

      Examples:
        | case        | level | severity |
        | an error    | ERROR | ERROR    |
        | information | INFO  | INFO     |
        | debug       | DEBUG | DEBUG    |

  Rule: A WARN event is written with Cloud Logging's spelling WARNING

    Scenario: A warning is written as WARNING
      When the broker logs an event at level WARN
      Then the entry severity is WARNING

  Rule: A TRACE event floors to DEBUG, the finest severity Cloud Logging has

    Scenario: Trace detail is written as DEBUG
      When the broker logs an event at level TRACE
      Then the entry severity is DEBUG
