# krabka-logfmt

Structured-JSON `tracing` log formatter shared across Krabka services (broker,
gRPC gateway, operator, schema-registry).

It installs a stdout `fmt` layer. The layer writes each event as one JSON object
on one line, in the shape that Google Cloud Logging (GKE) reads:

```json
{"timestamp":"2026-06-13T05:55:09.951788Z","severity":"DEBUG","target":"krabka_broker::network::dispatch","message":"connection opened","listener":"PLAIN","sasl":false}
```

- The layer maps `severity` from the `tracing` level (`WARN` → `WARNING`,
  `TRACE` → `DEBUG`), so Cloud Logging sets the entry's `LogSeverity`.
- The layer flattens `message`, the event fields and the fields of the span
  the event runs in to the top level. The message then becomes the entry
  summary, and the fields stay queryable. An event field wins over a span
  field of the same name.
- A field named like a key the layer writes itself (`timestamp`, `severity`,
  `target`, `fields`) or like a Cloud Logging special key (`httpRequest`,
  `time`, `logging.googleapis.com/...`) is kept under the `fields` object, so
  it is neither lost nor able to set the entry's own metadata.
- An integer whose magnitude exceeds 2^53 is written as a JSON string, so Cloud
  Logging, which stores numbers as doubles, keeps every digit.
- With a Google Cloud project configured, an entry written inside an
  OpenTelemetry span carries `logging.googleapis.com/trace`
  (`projects/<project id>/traces/<trace id>`), `logging.googleapis.com/spanId`
  and `logging.googleapis.com/trace_sampled`, so Cloud Logging links it to its
  trace. The span context comes from the `tracing-opentelemetry` layer, so this
  needs OTLP export on.
- The layer writes no ANSI colour codes. The default `tracing_subscriber` `fmt`
  layer adds colour even when stdout is not a terminal.

## Usage

```rust
use tracing_subscriber::{prelude::*, EnvFilter};

let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
tracing_subscriber::registry()
    .with(
        krabka_logfmt::layer()
            .filter(filter)
            .writer(std::io::stdout)
            // Optional: link entries to their trace in Cloud Trace.
            .gcp_project_id("my-project")
            .call(),
    )
    .init();
```
