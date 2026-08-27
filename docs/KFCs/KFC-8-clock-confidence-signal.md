# KFC-8: Clock Confidence as a First-Class Signal

A measured, queryable signal for how wrong each node's clock can be, and a record header that turns an ordinary produce into a clock measurement.

## Status

**Adopted.** The implementation is the clock block kind and the `/api/v1/clocks` ingest path in the metrics distributor. It also carries the shipped recording and alerting rule bundle, and the `krabka.hlc` record header in the client libraries. This document lands on branch `claude/clock-confidence-signal-w0veb8`.

No KIP defines clock telemetry, and no KIP bounds clock skew. [KFC-1](KFC-1-deliver-at-time-visibility.md) and [KFC-6](KFC-6-coordination-primitives-api.md) each declare a clock bound as a config value, and neither of them measures it. This document is the specification for the measurement. The Prometheus remote-write specification defines the envelope that the [ingest endpoint](#the-ingest-endpoint) reuses, and it says nothing about a clock reading.

## Motivation

krabka already treats wall-clock time as a safety parameter, and it already asks the operator to declare how wrong the clock can be. Nothing in the tree measures whether that declaration is true.

The declaration is a real shipped config. `LogConfig::delivery_clock_uncertainty` in `crates/log/src/config.rs` is the "declared bound on how far this broker's clock can be from true time", and `DEFAULT_DELIVERY_CLOCK_UNCERTAINTY` is `millis(250)`. `crates/log/src/log.rs` reads it in `delivery_settings`, and `crates/log/src/delivery.rs` adds it to a batch's activation time in `visible_at_ms`. The comment on that function states the rule in one line: "The bound is added, never subtracted."

KFC-1 builds its whole safety argument on that number. Call the broker's clock reading `c` and the declared bound `e`. KFC-1's [Activation](KFC-1-deliver-at-time-visibility.md#activation) section says true time lies between `c - e` and `c + e`, and it proves from that interval that delivery is never early.

[The Delivery Watermark](KFC-1-deliver-at-time-visibility.md#the-delivery-watermark) is the derived offset that the bound protects. It is the largest offset such that every batch below it is active, and a fetch stops there.

KFC-1's [ListOffsets](KFC-1-deliver-at-time-visibility.md#listoffsets) section states the cost of the same assumption on the other side. The watermark can move backwards by up to `2e` across a leader change. The old leader and the new leader can read clocks that differ by `2e` and both stay inside their declared bounds. KFC-1 accepts that cost and tells the operator to lower `delivery_clock_uncertainty` for a smaller one. An operator who lowers a bound that nobody measures moves the risk and does not remove it.

KFC-6 rests on the same number under a second name. It defines `coordination.clock.uncertainty.ms` with a default of `250`, and it says the config "carries the same meaning and the same default as `delivery_clock_uncertainty` in KFC-1". A client marks its leadership handle revoked at the lease deadline minus that bound, on its own clock, with no server round trip.

Nothing else in krabka bounds a clock. [KFC-4](KFC-4-cross-topic-snapshots.md) rejects timestamp-based cuts for exactly this reason: a snapshot built from record timestamps "is correct up to the skew, and nothing in Kafka bounds the skew". The declared bound is the only bound the project has, and it is a literal in a config file.

A clock breaks in ways that the declaration cannot see. A PTP grandmaster drops into holdover and its slaves drift apart at their oscillators' rates. An NTP daemon steps the clock and moves every future timestamp with it. Both faults break KFC-1 and KFC-6 at the same moment, and both are silent.

The only symptom today is the one KFC-1's [Metrics](KFC-1-deliver-at-time-visibility.md#metrics) section names. That section calls the activation lateness histogram "the one an operator watches". It says a rising tail "points at clock skew, or at a scheduler that does not get enough CPU". Both readings are guesses and the operator cannot separate them. The tail also appears only after the broker already delivered records under a bound that no longer held.

The broker has no clock abstraction that could report confidence. A grep for a `Clock` trait across `crates/` finds no definition at all. Three separate time sources sit in the tree instead.

`crates/broker/src/delivery/` and `crates/broker/tests/deliver_at_time.rs` use the external `qubit_clock::Clock` trait. `crates/broker/src/fetch_session.rs` and `crates/throttle/src/runtime.rs` use `qubit_clock::NanoMonotonicClock`. `crates/broker/src/heartbeat/controller_state.rs` declares a private `enum Clock` with a `Real` variant and a test variant, for liveness tracking alone. Each of them returns an instant. None of them returns how good that instant is.

Until this KFC the broker also published nothing an alert could compare against. `crabka-telemetry` builds an OTLP pipeline for spans and logs, and its `opentelemetry-otlp` dependency names the `trace` and `logs` features and not `metrics`. Broker metrics go out through `prometheus_client` in `crates/broker/src/metrics.rs`, and no series there carried the declared bound. This KFC adds one, so a rule reads the bound the broker relies on instead of a copy of it.

So two shipped designs rest on a number that an operator typed into a config file, and no code in the project ever compares that number against a clock. This KFC measures it.

## Public Interfaces

The feature adds one ingest endpoint, one metric block kind, fifteen metric names, one broker gauge, five recording rules, nine alerting rules, and one record header. It adds no Kafka api key, no error code, and no field to any Kafka request or response.

### The Ingest Endpoint

| Property | Value |
| :--- | :--- |
| Path | `POST /api/v1/clocks` on the metrics distributor |
| `Content-Encoding` | `snappy` |
| Body | A protobuf write request of clock readings, framed as the remote-write body is framed |
| Success | `204 No Content` |
| Body limit | The distributor's decompressed cap, applied per route |

The distributor already serves `/api/v1/push` and `/api/v1/write` for Prometheus remote write, and `require_snappy_encoding` gates both. The clock endpoint reuses that envelope, so an agent author reuses the remote-write toolchain that they already have. The message inside the envelope is a clock reading and not a sample, and the framing around it is unchanged.

krabka defines the endpoint and ships no agent. The daemon that knows the answer differs at every site, and it is `chronyd`, `ntpd`, `ptp4l`, `phc2sys`, or `gpsd`. Each of them already reports its own state, and an operator already runs the one they chose. A shipped agent would be a fifth opinion about a value that four daemons already hold, so this design takes the reading from whichever one is there.

The endpoint accepts a reading from any node, and the node id in the reading names the subject. A krabka broker sends its own readings, and a producer or a consumer host sends readings for the clock that stamps its records. The signal is a fleet signal and not a broker signal, because [the fleet bound](#the-shipped-recording-rules) is the number that matters and one node cannot compute it.

### What a Clock Reading Carries

| Term | Meaning |
| :--- | :--- |
| Node id | The krabka node that the reading describes. |
| Clock id | The clock on that node. A node with a hardware clock and a system clock sends two readings. |
| Source kind | `ptp`, `ntp`, `gnss`, `kernel_timex`, or `phc`. |
| Host reading | The wall-clock instant that the agent read on the host. |
| Uncertainty | The half-width of the interval around the host reading. |
| Offset | The signed difference from the reference clock. |
| Sync state | `synchronized`, `holdover`, `free_running`, `unsynchronized`, or `stepped`. |
| Reference identity | The grandmaster identity, the NTP peer address, or the GNSS receiver that the reading came from. |
| Last sync | The instant at which the clock last accepted a correction from its reference. |
| Frequency adjustment | The frequency correction that the daemon holds, in parts per billion. |
| Last step | The size of the last step that the daemon applied, and the instant it applied it. |
| NTP fields | Root delay and root dispersion. |
| PTP fields | Mean path delay, `stepsRemoved`, `gmClockClass`, and `gmClockAccuracy`. |
| Kernel fields | `maxerror`, `esterror`, and the `STA_UNSYNC` bit. |
| GNSS fields | The fix kind and the count of satellites used in the solution. |
| Receive instant | The instant at which the ingester received the reading. The ingester stamps it, and the agent cannot set it. |

The receive instant is the field that makes the reading self-checking. [Clock Telemetry Is a Signal, Not More Metrics](#clock-telemetry-is-a-signal-not-more-metrics) states why one coordinate is not enough.

### The Projected Metric Series

| Metric | Kind | Meaning |
| :--- | :--- | :--- |
| `krabka_clock_uncertainty_seconds` | gauge, per clock | Half-width of the interval around the host reading. |
| `krabka_clock_offset_seconds` | gauge, per clock | Signed difference from the reference clock. |
| `krabka_clock_sync_state` | gauge, per clock and state | `1` on the live state and `0` on every other state. |
| `krabka_clock_last_sync_seconds` | gauge, per clock | Unix time of the last correction that the clock accepted. |
| `krabka_clock_root_delay_seconds` | gauge, per clock | NTP round-trip delay to the root. |
| `krabka_clock_root_dispersion_seconds` | gauge, per clock | NTP dispersion accumulated to the root. |
| `krabka_clock_path_delay_seconds` | gauge, per clock | PTP mean path delay to the grandmaster. |
| `krabka_clock_class` | gauge, per clock | `gmClockClass` of the grandmaster that the node follows. |
| `krabka_clock_steps_removed` | gauge, per clock | `stepsRemoved` between the node and the grandmaster. |
| `krabka_clock_stratum` | gauge, per clock | NTP stratum of the peer that the clock follows. |
| `krabka_clock_frequency_ppb` | gauge, per clock | Frequency correction in parts per billion. |
| `krabka_clock_step_seconds_total` | counter, per clock | Cumulative size of the steps that the daemon applied. |
| `krabka_clock_ingest_skew_seconds` | gauge, per clock | Receive instant minus host reading. |
| `krabka_gnss_satellites_used` | gauge, per receiver | Satellites used in the current fix solution. |
| `krabka_gnss_fix` | gauge, per receiver | Fix kind: `0` none, `2` two-dimensional, `3` three-dimensional. |

The sync state rides as `krabka_clock_sync_state{state="holdover"} == 1`, which is the standard Prometheus idiom for an enumerated state. One series exists per state value, and exactly one of them reads `1` at any instant.

The projection is deliberately lossy. `gmClockAccuracy` and the reference identity stay in the block and get no series, because they are categorical values that would fork a series on every grandmaster failover.

`krabka_clock_uncertainty_seconds` is the one an operator watches, because it is the measured version of the number that KFC-1 and KFC-6 declare. `krabka_clock_sync_state` is the second, because a clock that left `synchronized` invalidates the first one.

There is deliberately no series that carries the age of a reading. An age computed when the reading arrives is zero at that moment and never grows, so the series would go stale instead of reporting staleness. Age is a question about the present, and `time() - timestamp(krabka_clock_uncertainty_seconds)` answers it against the moment the query runs.

### The Broker Gauge

The broker gains one series, on the registry it already runs in `crates/broker/src/metrics.rs`.

| Metric | Kind | Meaning |
| :--- | :--- | :--- |
| `delivery_clock_uncertainty_seconds` | gauge | The bound this broker declares, in seconds. It is `delivery_clock_uncertainty`, the extent KFC-1 adds to a batch's timestamp before the batch activates. |

The registry prefixes every broker series, so the exported name is `crabka_broker_delivery_clock_uncertainty_seconds`.

The value is a constant of a running broker, and no topic config overrides it. The broker publishes it once at startup, beside the delivery scheduler that reads the same config. It is the declared half of the comparison this KFC exists to make, and the measured half is `krabka_clock_uncertainty_seconds`.

### The Shipped Recording Rules

```yaml
groups:
  - name: krabka-clock
    interval: 15s
    rules:
      - record: krabka_clock:declared_bound_seconds
        expr: max(crabka_broker_delivery_clock_uncertainty_seconds)
      - record: krabka_clock:uncertainty_seconds:max
        expr: max(krabka_clock_uncertainty_seconds)
      - record: krabka_clock:fleet_skew_bound_seconds
        expr: |
          max(krabka_clock_offset_seconds + krabka_clock_uncertainty_seconds)
            - min(krabka_clock_offset_seconds - krabka_clock_uncertainty_seconds)
      - record: krabka_clock:uncertainty_budget_ratio
        expr: krabka_clock:uncertainty_seconds:max / krabka_clock:declared_bound_seconds
      - record: krabka_clock:unsynchronized_nodes
        expr: count(krabka_clock_sync_state{state!="synchronized"} == 1) or vector(0)
```

`krabka_clock:fleet_skew_bound_seconds` is the number the whole signal exists to produce. Each clock claims an interval from its offset minus its uncertainty to its offset plus its uncertainty. The largest upper end minus the smallest lower end is the largest difference between any two clocks in the cluster that the fleet's own uncertainty admits. That is the number an operator compares `delivery_clock_uncertainty` and `coordination.clock.uncertainty.ms` against. Two clocks each inside a bound `e` differ by at most `2e`, so a fleet bound above twice the declared bound says the declaration is false.

`krabka_clock:declared_bound_seconds` reads `crabka_broker_delivery_clock_uncertainty_seconds`, the gauge this KFC adds to the broker. The bound is a constant of a running broker and no topic config overrides it, so the broker publishes it once at startup. The rule takes the maximum across brokers, because a rolling config change leaves the cluster with two values for a short time and the larger one is the weaker promise. An operator who retunes `delivery_clock_uncertainty` changes nothing in the rule file, and the alert follows the broker.

### The Shipped Alerting Rules

| Alert | Fires on | Severity |
| :--- | :--- | :--- |
| `ClockUnsynchronized` | `krabka_clock_sync_state{state="unsynchronized"} == 1` for 1m | critical |
| `ClockUncertaintyExceedsDeclaredBound` | `krabka_clock_uncertainty_seconds > on() group_left() krabka_clock:declared_bound_seconds` for 2m | critical |
| `ClockFleetSkewExceedsDeclaredBound` | `krabka_clock:fleet_skew_bound_seconds > 2 * krabka_clock:declared_bound_seconds` for 2m | critical |
| `ClockUncertaintyBudgetHigh` | `krabka_clock:uncertainty_budget_ratio > 0.5` for 10m | warning |
| `ClockStepped` | `increase(krabka_clock_step_seconds_total[5m]) > 0` | warning |
| `ClockInHoldover` | `krabka_clock_sync_state{state="holdover"} == 1` for 5m | warning |
| `PtpGrandmasterFlapping` | `changes(krabka_clock_class[15m]) > 2` | warning |
| `GnssFixLost` | `krabka_gnss_fix == 0` for 5m | warning |
| `ClockTelemetryStale` | `time() - timestamp(krabka_clock_uncertainty_seconds) > 120 or absent(krabka_clock_uncertainty_seconds)` for 5m | critical |

`ClockTelemetryStale` is not optional, and it is the reason the signal carries a reading age at all. A clock agent that stopped sending looks exactly like a clock that is healthy and unchanging. Every other alert in this table goes quiet at the same moment, so the absence of readings has to be the loud condition.

### The Record Header

| Property | Value |
| :--- | :--- |
| Key | `krabka.hlc` |
| Length | 16 bytes, fixed |
| Bytes 0 to 7 | Wall component, microseconds since the Unix epoch, big-endian signed |
| Bytes 8 to 11 | Logical counter, big-endian unsigned |
| Bytes 12 to 15 | Node id of the writer, big-endian signed |

The layout uses fixed-width big-endian integers and no varint framing. The reason is the one KFC-6 gives for the `__coordination_state` value layout. The Java and Go clients decode it by hand. A hand-written decoder for three fixed offsets is a decoder that stays correct.

## Proposed Changes

### Clock Telemetry Is a Signal, Not More Metrics

The metrics store already holds float samples, native histograms, exemplars, and metadata. A clock reading looks at first like a fifth kind of sample. It is not, and four properties separate it.

**The sample's own timestamp is the thing under measurement.** An ordinary metric sample carries one timestamp, and every reader trusts it. A clock reading is a claim about that timestamp, so it needs two coordinates: the instant the host read, and the instant the ingester received. A reading with one coordinate cannot show a host that is one second ahead. The host writes the wrong instant into the only field that says when the reading happened.

**The value is an interval and not a point.** The offset and the half-width together are the reading. Split across two float series, a query joins them at two scrape times, and it can pair an offset from one moment with a half-width from another. The interval that comes out never existed on any host, and it can be narrower than the truth, which is the direction that hides a fault.

**The state is categorical and it carries the safety meaning.** `holdover` and `synchronized` are not two values of one number. A clock in holdover reports a small uncertainty for as long as its daemon still trusts its own oscillator model, so the number alone says the fleet is healthy. The state is what says the number is stale.

**The reference identity churns.** A PTP grandmaster failover changes the reference while the node and the clock stay the same. Carried as a metric label, that change forks every series at that instant, so a range query across the failover returns two broken halves rather than one continuous reading. The reference identity belongs in the row and not in the label set.

### The Ingester Times the Reading, Not the Agent

A metrics ingester normally takes the sample timestamp from the sender. This one cannot, and the reason is the whole point of the signal.

A host that is an hour ahead stamps its reading an hour into the future. Stored at that timestamp, the reading lands an hour ahead in the block. Every range query over the last five minutes then misses it, and the one clock that needs an alert is the one clock that has no samples. So the ingester stores each reading at its own receive instant, and it keeps the host reading as a value in the row.

The difference between the two becomes `krabka_clock_ingest_skew_seconds`. That series is the crudest measurement in the signal and also the most robust one. It needs nothing from the host except that the host sent something. It holds when the agent misreports its own uncertainty, and it holds when the agent reports a state that its daemon does not agree with.

The ingester stores a reading whose host timestamp is far from the receive instant, and it does not refuse it. A refusal would drop exactly the reading that carries the fault, and it would leave the operator with silence and a `ClockTelemetryStale` alert instead of a number.

### The Block Is the Source of Truth

The metrics store lives in the observability repository, and every path in this section is a path there. `MetricBlockKind` in `crates/metrics/src/compactor.rs` names four block kinds today: `Float`, `NativeHistograms`, `Exemplars`, and `Metadata`. This design adds a fifth for clock readings, with its own columnar schema beside the ones in `crates/metrics/src/schema.rs` and its own object path in the deterministic key layout.

A new block kind is cheap because everything around it is shared. Ingest appends to the same metrics WAL topic, `__crabka_metrics_wal`. The compactor writes the same block and index sidecar pair, and it commits the same manifest. Tenancy and the limits enforcement apply with no new code.

One row holds one reading. The offset, the half-width, the sync state and the reference identity stay together in that row, and no query can separate them. That atomicity is the single property the new block kind exists for, and it is the property that a set of independent float series cannot give.

### What the Signal Costs

The volume is small, and it is worth stating so that nobody sizes it wrong.

An agent sends one reading per clock per interval, and the default interval is 10 seconds. A cluster of 100 nodes with two clocks on each node sends 20 readings per second. A reading is a few hundred bytes, so the ingest rate is a few kilobytes per second. The projection is about 20 series per clock, because the sync state contributes one series per state value, which gives about 4,000 series for that cluster. Both numbers grow with the fleet and neither grows with the traffic.

The record header is the cost that does grow with traffic, and it falls on the client. The stamp is 16 bytes of value and the key `krabka.hlc` is 10 bytes. The varint framing around them adds a few more, so a stamped record grows by about 30 bytes before compression. On a 200-byte record that is a 15 percent increase, and on a 20-byte record it is more than double. A producer that stamps small records pays a real price. A producer that wants the measurement without the price stamps a sample of its records rather than all of them.

### The Query Face Is a Projection, Not a New Query Language

Ingest also writes ordinary float samples for each reading. Those samples are the series in [The Projected Metric Series](#the-projected-metric-series), and they are a derived view of the block.

KFC-6 makes the same split for `__coordination_state`, and it says so in one line: the topic "is a projection and not the source of truth". The lag is safe here for the same shape of reason. A reader that needs the exact reading reads the block, where the interval and the state are still atomic. A reader that needs to alert reads the projection, and an alert that fires one evaluation interval later is still the same alert.

The gain is the entire query path with no change to it. PromQL, the ruler, `/api/v1/rules`, `/api/v1/alerts`, and Grafana all work on the projected series, because the projected series are ordinary float series. An operator writes a clock query with the language they already write every other query in.

### Extend the Signal Where PromQL Cannot Express Something

`krabka_clock_last_sync_seconds` is the worked example. An operator asks how long a clock has been in holdover, and PromQL has no function that answers it.

The answer is a coordinate and not a function. The signal exports the last-sync instant as a Unix timestamp, so holdover duration is `time() - krabka_clock_last_sync_seconds` in plain PromQL, with no extension at all.

This is the rule for every gap of that shape. Add the coordinate that the question needs, and keep the grammar exactly as Prometheus defines it. [New PromQL Functions for Skew](#new-promql-functions-for-skew) states what a new function costs.

### HLC Stamps Ride a Record Header

A hybrid logical clock stamp is per-record state, and Kafka persists exactly one per-record carrier that every client can read.

The v2 record format leaves no other place. `Record` in the sibling `krabka-protocol` repository, at `crates/protocol/src/records/owned.rs`, carries an attributes byte, a timestamp delta, an offset delta, a key, a value, and headers. It has no tagged-fields trailer, and `Record::decode` rejects trailing bytes inside the record's claimed length with a `RecordParse` error. A stamp appended after the header list is a decode failure on every conforming client, krabka's own included.

So the header is the carrier. Record headers arrived with the v2 format in Kafka 0.11, and every client since then reads and writes them. The broker stores a header inside the record data, as it stores the key and the value.

### The Broker Never Parses the Header

A record header lives inside the record data, under the batch's compression.

KFC-1 rejected a `krabka.deliver.at` header for exactly that reason, and its words are worth keeping: "The produce hot path rules it out. A header lives inside the record data, under the batch's compression, so the broker would have to decompress and decode every batch to find it."

This design honours that objection rather than reopening it. The producer writes `krabka.hlc` and the consumer reads it. The broker writes the producer's bytes through without a look inside, so zero-copy reads and byte-exact record passthrough stay exactly as they are. No handler in `crates/broker/` gains a decompress.

The log's own `StampSource` seam in `crates/log/src/stamp_source.rs` is a different mechanism, and this KFC does not change it. That seam allocates an internal server-side coordinate for the `.stampindex`, and its documentation already names the "Lamport/HLC receive rule" for `observe`. It never reads a record header, and it never will under this design.

### A Received Stamp Is Refused, Not Absorbed

The receive rule runs in the client, and it has a ceiling.

A receiver compares the stamp's wall component against its own clock reading. Below the configured ceiling, the receiver takes the usual hybrid logical clock step and moves its wall component forward. Above the ceiling, the receiver refuses the stamp and keeps its own clock. It counts the refusal, and it still delivers the record to the application.

The classic receive rule absorbs any stamp that is ahead. That rule lets one broken clock drag the whole fleet forward, and the damage does not stop. Every receiver then stamps its own records with the wrong value and passes it on. A ceiling turns one broken clock into one alert and a bounded count of refusals.

The ceiling is the self-fence: a client protects its own clock and waits for nobody else to protect it. It is a client-side rule and the broker enforces nothing. A client that ignores it harms itself and the clients that read what it produces.

### Every Produce Is a Passive Clock Probe

A stamp that pulls a receiver's clock forward by 40 ms is direct evidence of 40 ms of skew between the producer and that receiver.

That measurement needs no exporter, no extra request, and no reference clock. An exporter measures a clock against a reference that the exporter chose, at the rate the exporter polls. A stamp measures two nodes that really exchange data, at the moment they exchange it, at the rate they exchange it. Those pairs are the ones whose skew can hurt an application, because they are the ones that share records.

The client reports each forward pull and each refusal to the same ingest endpoint, so both halves land in one signal and one set of alerts. This is why the telemetry half and the stamp half are one KFC and not two. The stamp half is a second measurement path for the same quantity, and it reaches producers and consumers that no exporter runs on.

The stamp half is weaker in two ways, and the telemetry half covers both. A stamp measures the difference between two clocks and never the error of either one. Two nodes that are both an hour behind agree perfectly and report nothing. A stamp also only measures a pair that exchanges data, so a silent producer is invisible to it. The exporter reading carries a reference and an uncertainty, and it arrives whether or not the node produces anything.

### What This Does Not Do

Three limits belong here plainly, because a reader can otherwise take this feature for more than it is.

**It does not make any clock correct.** It reports what the clock daemons already know, and it makes that report queryable and alertable. A node with a broken clock still has a broken clock after this change.

**It changes no KFC-1 and no KFC-6 behaviour.** The delivery watermark, the activation rule, the lease arithmetic, and the fence-before-grant order are all exactly what those documents specify. `delivery_clock_uncertainty` and `coordination.clock.uncertainty.ms` keep their meanings and their defaults.

**It does not let the broker refuse work when the bound is violated.** A broker whose measured uncertainty reaches a full second keeps activating batches at 250 ms. It keeps granting leases as well. The signal fires an alert, and an operator acts on it. [Making the Broker Refuse Work When the Bound Is Violated](#making-the-broker-refuse-work-when-the-bound-is-violated) states why that change belongs in its own KFC.

The feature reports. It does not decide.

## Compatibility, Deprecation, and Migration Plan

Nothing in Kafka's wire protocol changes. No api key gains a version, no request or response gains a field, and no Kafka error table gains a value.

The record header is ordinary Kafka. Headers are part of the v2 record format that Kafka 0.11 shipped. A client that does not know the key `krabka.hlc` reads past it, exactly as it reads past any other application header. A topic whose producers write no stamp is byte-for-byte what it is today.

The projection adds sixteen metric names under the `krabka_clock_` and `krabka_gnss_` prefixes. A dashboard or a rule that does not name them sees no change, and [What the Signal Costs](#what-the-signal-costs) gives the series count those names produce.

The ingest endpoint is a new route on the distributor router. `/api/v1/push`, `/api/v1/write`, and the two OTLP routes keep their paths and their behaviour.

The clock block kind is a new value of `MetricBlockKind`, so it changes the set of block kinds on disk and in the manifest. krabka is greenfield and undeployed, so nothing migrates. Delete the local block store and the metrics WAL during development if the old set is in the way.

The work reaches one sibling repository, and it reaches no wire schema. `krabka-client-rs` gains the code that writes and reads `krabka.hlc`, and `krabka-client-java` and `krabka-client-go` gain the same. `krabka-protocol` needs no change, because `RecordHeader` already carries a string key and an optional byte value, and the header list already round-trips through `Record::encode` and `Record::decode`.

An operator who runs no clock agent gets no readings, and `ClockTelemetryStale` fires. That is the intended behaviour and not a defect. The rest of the bundle stays quiet, because a rule with no input series produces no alert.

## Test Plan

Six tiers cover the feature.

**Property.** Proptest round-trips the record header. Any wall component, counter, and node id encode to 16 bytes and decode back to the same three values. Any 16-byte input either decodes or fails cleanly. The clock reading codec gets the same treatment across every source kind and every sync state.

**Table-driven.** One case covers each branch of the receive rule. Five branches exist: the stamp behind the local wall component, equal to it with a lower counter, equal to it with a higher counter, ahead inside the ceiling, and ahead past the ceiling. Each case compares the whole resulting stamp against an expected value, and not one field at a time.

**Physical clock.** A manual clock drives the self-fence and the monotonicity of a receiver's own stamps. A test that moves the clock by hand turns each of them into a deterministic assertion, and not a race against real time. This is the tier KFC-1 uses for activation, and for the same reason.

**Schema.** The clock block's Arrow schema is validated against the block declaration. A column added on one side and not the other then fails the build rather than a query.

**Ruler.** The shipped bundle is loaded and evaluated against seeded readings. One test moves those readings across a threshold and asserts that an alert goes from inactive to pending and then to firing. That path proves the `for` extent as well as the expression. The bundle is tested by loading and evaluating it, and never by asserting on its file text.

**Conformance.** The vendored Prometheus `.test` corpus under `crates/promql/tests/testdata` in the observability repository stays green **unchanged**. An unchanged corpus is the whole value of that tier: it is the standing check that this feature added a signal and changed no grammar.

## Rejected Alternatives

### Clock Readings as Ordinary Metrics

Two float series per clock, one for the offset and one for the uncertainty, needs no new block kind and no new endpoint. It is the first design a reader proposes, and it loses on all four properties in [Clock Telemetry Is a Signal, Not More Metrics](#clock-telemetry-is-a-signal-not-more-metrics).

The interval is the sharpest of them. A query that joins the two series joins them by label set, and the two samples it finds can come from two scrape times. The interval it builds never existed, and it can be narrower than the truth, so the failure mode of this design is a clock fault that reads as healthy.

The reference identity is the second. Carried as a label it forks every series on a grandmaster failover, which is the exact moment an operator needs a continuous series to look at.

### New PromQL Functions for Skew

A `clock_skew()` function reads well and puts the fleet arithmetic in one place.

It breaks portability. The vendored Prometheus conformance corpus is the project's proof that krabka's PromQL is Prometheus' PromQL, and a grammar extension makes that proof local rather than shared. Every query written against a krabka extension also stops working on Prometheus, Mimir, and Thanos, which is the portability an operator picked PromQL for.

It also buys nothing. `max(offset + uncertainty) - min(offset - uncertainty)` is the fleet bound, it is one line of standard PromQL, and it ships as a recording rule so nobody types it twice.

### A Batch Attribute Bit Carrying the HLC

Bits 7 to 15 of the v2 batch attributes word are free. `Attributes` in `krabka-protocol`, at `crates/protocol/src/records/header.rs`, uses bits 0 to 2 for the codec, bit 3 for the timestamp type, bit 4 for transactional, bit 5 for control, and bit 6 for the delete horizon. A spare bit looks like an invitation.

The attributes word sits inside the CRC region. The v2 CRC covers everything after the `crc` field of the header, and the attributes word is the first field after it. A JVM consumer reads that word with the full 16 bits and maps the values it knows. A krabka bit in that word changes what a stock client believes about the batch. Byte exactness against Apache Kafka is the constraint that outranks the convenience here.

A batch attribute also carries the wrong granularity. One batch holds many records from one producer at one instant, and the stamp the receiver needs is per record.

### The Broker Advancing Its Own HLC From Produced Records

The broker could read `krabka.hlc` on the produce path and fold each stamp into a broker-side clock. Every produce would then improve the broker's own view with no client cooperation.

It costs a decompress for every batch on the write path. That is the objection KFC-1 raised against `krabka.deliver.at`, and it is no weaker here. A header lives under the batch's compression, so finding it means decoding every record the cluster accepts. The broker's byte-exact passthrough and its zero-copy read path both depend on never looking inside.

The measurement is available without that cost. The receiving client already computes the pull, and it already sends it to the ingest endpoint.

### A Separate Clock Signal Crate

A standalone service for clock readings would keep the metrics store untouched and give the signal its own release cadence.

It duplicates the parts that took the most work and gains nothing for them. It needs its own write-ahead log, its own compactor, its own manifest and object key layout, its own tenancy, and its own limits enforcement. At the end of that work the query language is still PromQL, because the alerts are PromQL alerts and the dashboards are Grafana dashboards.

A block kind reuses all of it. The one thing the new kind does not share is the columnar schema, which is the one thing that genuinely differs.

### Deriving the Signal From `node_exporter` timex Series on Remote Write

`node_exporter` already exports `node_timex_maxerror_seconds`, `node_timex_offset_seconds`, and `node_timex_sync_status`. A recording rule over the existing remote-write path could assemble a reading from them with no new code at all.

It reassembles one reading out of series that were scraped independently. The interval and the state can then come from different moments, which is the failure this design exists to remove. A clock that stepped between two scrapes reports a pre-step offset beside a post-step state, and the pair looks healthy.

The coverage is also wrong in two directions. `timex` describes the kernel's system clock, so it carries no PTP grandmaster identity, no `stepsRemoved`, no path delay, and no GNSS fix. It also describes only hosts that run `node_exporter`, and the producers and consumers that this design measures through stamps usually do not.

### Trusting the Producer's HLC Unconditionally

The classic hybrid logical clock receive rule takes any stamp that is ahead and moves the local clock to it. It is simple, it is well studied, and it gives the strongest causality guarantee.

It has no upper bound on the damage from one fault. A single producer whose clock jumps a year forward stamps one record, and every receiver of that record moves a year forward. Those receivers stamp their own records with the new value. The fault then spreads at the rate the cluster exchanges data, and it never recovers on its own.

The ceiling gives up a small amount of causality tracking for a bounded fault. A refused stamp means the receiver keeps a stamp that is behind a record it has seen, and that is visible and countable. A fleet dragged a year forward is neither.

### Making the Broker Refuse Work When the Bound Is Violated

The strongest version of this feature would have the broker read its own measured uncertainty and stop acting when the measurement exceeds the declared bound. A broker would refuse to activate a scheduled batch, and a controller would refuse to grant a lease.

That is a much larger change than a signal, and it changes the contracts of two adopted documents. KFC-1 promises that a scheduled record is delivered once it is durable, and a refusal turns a clock fault into an unbounded delivery stall. KFC-6 prefers a vacant role over two writers. It reaches that state through a fence and a lease expiry, and not through a clock reading that arrives from outside the metadata quorum.

It also needs answers that this document does not have. What does the broker do with a reading that is itself stale? How does a measurement reach the broker without a new dependency from the data plane onto the metrics store? What does an operator do with a cluster that refuses every produce?

Each of those is a design question with its own rejected alternatives. Each one belongs in its own KFC, and this document would bury all three.
