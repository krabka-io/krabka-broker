# Share DLQ minimum-feature differential

Use fresh caller-owned isolated brokers and the pinned Kafka4.4 released jars.
The oracle and explicitly enabled candidate must begin at finalized
`share.version=1`. The fixture upgrades them to2; do not reuse a broker already
holding level2 test state or downgrade such a broker to accommodate this fixture.
The default candidate uses its supported default metadata level and share1.
Record image digest, source revision, executable digest, configuration and owned
lifecycle commands alongside the task receipt.

```
aspect kafka44-share-minimum --oracle 127.0.0.1:64820 \
  --enabled 127.0.0.1:64822 --default 127.0.0.1:64824 \
  --libs /absolute/released/libs --output /fresh/private/receipt
aspect tests kafka44-share-minimum
```

Each enabled endpoint creates valid DLQ topic/group configuration at level1,
produces and rejects one exact source record, and observes no DLQ record during
an eight-second bounded window. At level2, fresh topic/group names produce one
DLQ record with the exact original key/value and offset0. Both phases require
zero share lag. The default endpoint must reject the level2 feature update with
`InvalidUpdateVersionException` and retain finalized level1.

The retained `qualification/kafka44-dlq-minimum-20261104.json` records direct
executions of the same compiled fixture against a dedicated immutable oracle,
historical immutable enabled candidate, and historical default candidate. It
also retains initial provisioning failures, corrected formats and raw responses.
The AXL parser's positive and negative controls passed; full AXL orchestration
was not executed for that receipt. No denied-destination, multibroker, crash or
current-integration-build claim is made. `qualification_pass` remains false.
