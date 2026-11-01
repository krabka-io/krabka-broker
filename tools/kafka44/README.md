# Released Kafka 4.4 evidence

Issue [#1969](https://github.com/krabka-io/krabka-broker/issues/1969) requires released artifacts, not the moving `tools/kafkatest/suite.json` source revision. `collect.py` starts a disposable combined-KRaft reference broker and uses the same immutable **Linux amd64** Apache Kafka 4.4.0 image for reference and candidate tools. It records raw commands, outputs, exit codes, reference configuration, client version, candidate revision, and SHA-256 hashes of candidate configuration inputs.

This is the first evidence lane, **not a qualification suite**. A zero exit code means collection succeeded; `qualification_pass` remains false. No output normalization silently converts API, feature, or configuration differences into parity passes. Review raw results before drawing conclusions. The collector never upgrades candidate features or changes candidate configuration.

## Run

Requires Python 3, Docker, Linux amd64, and permission to run containers. Oracle-only collection is useful before provisioning candidates:

```sh
python3 tools/kafka44/collect.py --revision FULL_KRABKA_COMMIT_SHA --output evidence/kafka44-oracle
```

For candidate evidence, start two isolated brokers from the **same recorded revision**: one with default compatibility gates, and one explicitly configured with the candidate API/feature gates and intended metadata/feature levels. Supply their complete effective configuration files (retain these privately; only their checksums enter evidence):

```sh
python3 tools/kafka44/collect.py \
  --revision FULL_KRABKA_COMMIT_SHA \
  --default-bootstrap localhost:19092 \
  --enabled-bootstrap localhost:29092 \
  --default-config /private/default.toml \
  --enabled-config /private/enabled.toml \
  --output evidence/kafka44-comparison
```

Candidate tools run with host networking; advertised broker addresses must be reachable there. Bootstrap addresses must identify the prepared candidates, not production. The output directory must not already exist. Review output for private endpoint/configuration information before attaching it publicly. Client properties/authentication are not yet supported; authorization is explicitly unqualified.

`evidence.json` contains API ranges, feature levels, and broker-default configuration discovery from each requested side. Timeouts and tool failures produce a nonzero status and retained output. The owned reference container is removed on normal completion or handled errors. If the process is killed, remove only the container named by the recorded startup command.

## Remaining acceptance criteria

Keep #1969 open until released-client/protocol differential scenarios verify config validation and errors, authorization, minimum feature boundaries, default/enabled behavior, restart/recovery, and relevant multi-broker paths for all six surfaces: share DLQ, delayed tiered uploads, misrouting detection, controller unregistration, topic-ID transactional offset commits, and Streams topology APIs. Existing Rust/unit coverage and trunk system-test inventory are useful inputs, not released-4.4 execution evidence. Library-only, Connect and shell-tool features are outside broker parity unless they expose a broker obligation.

## Observed baseline

[`qualification/kafka44-oracle-e201a1d.json`](../../qualification/kafka44-oracle-e201a1d.json) records a successful local Linux amd64 reference-only collection on 2026-11-01. The released client reported `4.4.0`; all three probes and owned-container cleanup returned zero. The recorded Krabka revision identifies the source inventory under review, **not an executed candidate**: only `sides.oracle` exists. Verbose reference startup logs are omitted from this checked-in snapshot. Candidate interoperability and all listed feature scenarios remain unexecuted.
