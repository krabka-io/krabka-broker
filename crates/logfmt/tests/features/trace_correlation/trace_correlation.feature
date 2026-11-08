Feature: Entries link to their trace in Cloud Trace

  Cloud Logging links an entry to its trace in Cloud Trace through three keys:
  logging.googleapis.com/trace names the trace as projects/<project
  id>/traces/<trace id>, logging.googleapis.com/spanId names the span, and
  logging.googleapis.com/trace_sampled says whether the trace was sampled. The
  formatter writes them from the OpenTelemetry span the event runs in, so they
  need OTLP export on and a Google Cloud project configured.

  Depends on: Entries carry their time and origin under keys the formatter owns - ../entry_keys/entry_keys.feature

  Rule: With a project configured, an entry inside a traced span names its trace, its span and the sampling decision

    Scenario: An entry inside a sampled request span links to its trace
      Given the broker has been configured for Google Cloud project krabka-prod
      And the broker is tracing a produce request as span 00f067aa0ba902b7 of trace 4bf92f3577b34da6a3ce929d0e0e4736
      When the broker logs that a batch was appended
      Then the entry trace keys read
        """
        {
          "logging.googleapis.com/trace": "projects/krabka-prod/traces/4bf92f3577b34da6a3ce929d0e0e4736",
          "logging.googleapis.com/spanId": "00f067aa0ba902b7",
          "logging.googleapis.com/trace_sampled": true
        }
        """

    Scenario: An entry inside a span the sampler dropped is marked not sampled
      Given the broker has been configured for Google Cloud project krabka-prod
      And the broker is tracing a produce request that the trace sampler dropped
      When the broker logs that a batch was appended
      Then the entry is marked as not sampled

  Rule: Without a project configured, entries carry no trace keys

    Scenario: An entry inside a traced span carries no trace keys when no project is configured
      Given the broker is tracing a produce request as span 00f067aa0ba902b7 of trace 4bf92f3577b34da6a3ce929d0e0e4736
      When the broker logs that a batch was appended
      Then the entry carries no trace keys

  Rule: An entry outside a traced span carries no trace keys

    Scenario: An entry outside any span carries no trace keys
      Given the broker has been configured for Google Cloud project krabka-prod
      When the broker logs that a connection opened
      Then the entry carries no trace keys

    Scenario: An entry inside a span that OTLP does not trace carries no trace keys
      Given the broker has been configured for Google Cloud project krabka-prod
      And the broker is handling a produce request for topic orders
      When the broker logs that a batch was appended
      Then the entry carries no trace keys

  Rule: The trace keys come from the span, not from an event field

    Scenario: An event field cannot replace the trace an entry links to
      Given the broker has been configured for Google Cloud project krabka-prod
      And the broker is tracing a produce request as span 00f067aa0ba902b7 of trace 4bf92f3577b34da6a3ce929d0e0e4736
      When the broker logs an event with a field named logging.googleapis.com/trace of forged
      Then the entry field logging.googleapis.com/trace reads projects/krabka-prod/traces/4bf92f3577b34da6a3ce929d0e0e4736
